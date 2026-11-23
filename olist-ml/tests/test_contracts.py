"""The Spark job, the Gold DDL, the ClickHouse DDL/load and the Python package must agree (no Spark/ClickHouse needed)."""
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
LAKE = REPO / "olist-lakehouse"
sys.path.insert(0, str(ROOT))
from olist_ml.predict_core import FORECAST_COLUMNS, ORDER_PRED_COLUMNS, RAW_COLUMNS   # noqa: E402

TABLES = {"order_predictions": ORDER_PRED_COLUMNS, "forecast_daily_orders": FORECAST_COLUMNS}
CH_OF = {"STRING": "String", "BIGINT": "Int64", "INT": "Int32", "DOUBLE": "Float64", "DATE": "Date", "TIMESTAMP": "DateTime64(6)"}


def read(p) -> str:
    return Path(p).read_text(encoding="utf-8").replace("\r\n", "\n")


def columns(block):
    out = []
    for ln in block.strip().splitlines():
        ln = ln.split("--")[0].strip().rstrip(",")
        if ln:
            name, typ = ln.split(None, 1)
            out.append((name, typ.strip()))
    return out


def test_gold_ddl_columns_match_the_python_contract():
    ddl = read(LAKE / "layers/gold/create_gold_tables.sql")
    for t, cols in TABLES.items():
        block = re.search(rf"CREATE TABLE IF NOT EXISTS iceberg\.gold\.{t} \((.*?)\) USING iceberg", ddl, re.S).group(1)
        assert [n for n, _ in columns(block)] == cols, t


def test_clickhouse_tables_have_the_same_columns_and_compatible_types():
    ice = read(LAKE / "layers/gold/create_gold_tables.sql"); ch = read(LAKE / "layers/clickhouse/init_gold.sql")
    for t, cols in TABLES.items():
        iceberg = columns(re.search(rf"CREATE TABLE IF NOT EXISTS iceberg\.gold\.{t} \((.*?)\) USING iceberg", ice, re.S).group(1))
        click = columns(re.search(rf"CREATE OR REPLACE TABLE olist_gold\.{t}__new \((.*?)\) ENGINE", ch, re.S).group(1))
        assert [n for n, _ in click] == cols == [n for n, _ in iceberg], t
        for (n, it), (_, ct) in zip(iceberg, click):
            assert re.sub(r"Nullable\((.*)\)", r"\1", ct) == CH_OF[it], (t, n, it, ct)
        assert f"CREATE TABLE IF NOT EXISTS olist_gold.{t} AS olist_gold.{t}__new;" in ch


def test_clickhouse_load_swaps_every_loaded_table_and_only_after_all_are_loaded():
    sql = read(LAKE / "layers/clickhouse/load_gold.sql")
    loaded = re.findall(r"INSERT INTO olist_gold\.(\w+)__new SELECT \* FROM s3\('http://minio:9000/lakehouse/exports/gold/(\w+)/\*\.parquet'", sql)
    swapped = re.findall(r"EXCHANGE TABLES olist_gold\.(\w+)__new AND olist_gold\.(\w+);", sql)
    assert all(a == b for a, b in loaded) and sorted(a for a, _ in loaded) == sorted(a for a, _ in swapped) and len(loaded) == 10
    assert set(TABLES) <= {a for a, _ in loaded}
    assert sql.rindex("INSERT INTO") < sql.index("EXCHANGE TABLES")                      # all-or-nothing, as before


def test_spark_job_selects_exactly_the_columns_the_python_core_expects_and_writes_the_gold_tables():
    src = read(LAKE / "layers/predict/build_predictions.py")
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "order_features")
    selects = [c for c in ast.walk(fn) if isinstance(c, ast.Call) and getattr(c.func, "attr", "") == "select"
               and all(isinstance(a, ast.Constant) for a in c.args) and len(c.args) > 10]
    assert selects and [a.value for a in selects[-1].args] == RAW_COLUMNS
    assert '"order_predictions"' in src and '"forecast_daily_orders"' in src and "export_table(spark, table)" in src
    top = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    assert not any("olist_ml" in ast.dump(n) for n in top)                                  # lazy: other stages never need sklearn
    build = read(LAKE / "layers/gold/build_marts.py")
    for name in ("def conform", "def export_table", "GOLD =", "SILVER ="):
        assert name in build, name


def test_pipeline_registers_the_stage_lazily_and_the_batch_script_runs_it_after_gold():
    pipe = read(LAKE / "olist_pipeline.py")
    assert '"predict": lambda spark, a: _predict(spark)' in pipe and "from layers.predict.build_predictions import" in pipe
    assert "from layers.predict" not in pipe.split("def _predict")[0]                      # not imported at module level
    bat = read(LAKE / "run_lakehouse.bat")
    assert "gold predict clickhouse" in bat and "predict" in bat.split("echo   gold")[1][:300]


def test_spark_container_has_the_libraries_the_mounts_and_the_data():
    dock = read(LAKE / "infrastructure/spark/Dockerfile"); comp = read(LAKE / "infrastructure/docker-compose.yml")
    assert "python3-pip" in dock and re.search(r"pip3 install .*pandas.*scikit-learn", dock)
    master = comp[comp.index("  spark-master:"):comp.index("  spark-worker:")]
    for needle in ("PYTHONPATH: /opt/spark:/opt/olist-ml", "../../olist-ml/olist_ml:/opt/olist-ml/olist_ml:ro",
                   "../../olist-ml/artifacts:/opt/olist-ml/artifacts", "../../hosting_server/Data:/data:ro"):
        assert needle in master, needle
    assert (REPO / "olist-ml/olist_ml/predict_core.py").exists() and (REPO / "hosting_server/Data/olist_orders_dataset.csv").exists()
