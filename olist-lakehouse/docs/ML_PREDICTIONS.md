# Dự đoán ML trong Lakehouse (stage `predict`)

Dành cho: phụ trách Lakehouse, phụ trách Power BI. Code mô hình: `olist-ml/`. Stage Spark: `layers/predict/build_predictions.py`.

## 1. Vị trí trong pipeline
```
bronze → silver-dimensions → silver-facts → gold → predict → clickhouse → Power BI
                                                     ▲
              trọng số: MinIO  lakehouse/models/late_risk/<version>/   (train riêng, xem mục 4)
```
`predict` **chỉ đọc Silver + trọng số, chỉ ghi 2 bảng mới** (`gold.order_predictions`, `gold.forecast_daily_orders`) rồi export Parquet
như các mart khác; không đụng bảng gold nào có sẵn. Nạp ClickHouse: 10 bảng (8 cũ + 2 mới), vẫn all-or-nothing rồi `EXCHANGE`.
Vì Silver/Gold rebuild toàn bộ mỗi lần chạy nên `predict` cũng **ghi đè toàn bộ** hai bảng (idempotent).

## 2. Quyết định train / suy luận
| Câu hỏi | Quyết định | Lý do |
|---|---|---|
| Train theo batch hay gộp? | **Train ngoài pipeline, gộp toàn bộ lịch sử đến mốc T**; pipeline chỉ suy luận | batch nhỏ, nhãn đến muộn (~12 ngày giao hàng), tỷ lệ trễ 1–17% |
| Chia dữ liệu | **Theo thời gian** (train < T, kiểm tra sau T), không chia ngẫu nhiên | tránh lộ tương lai |
| Chia theo tháng? | Chỉ để **đánh giá** từng giai đoạn, không train mỗi tháng một mô hình | mỗi tháng ~5–8k đơn, rất ít đơn trễ |
| Train lại | **Mỗi tháng** (hoặc tối thiểu mỗi quý): train mốc T mới → publish phiên bản mới → đổi `current.json` | đo được AUC 0,62–0,77 tùy giai đoạn, tỷ lệ trễ nền 7% → 1,4% |
| Dự báo số đơn | Không có trọng số: mỗi lần chạy **fit lại** (~1 giây); chỉ độ rộng nón lấy từ `forecast_bands.json` | mô hình nhẹ, dữ liệu ngắn |
| Cờ cảnh báo | **Theo thứ hạng**: top 10% / top 2,5% điểm rủi ro của đơn trong 3 ngày trước, không nhìn tương lai | ngưỡng % cố định sẽ trôi khi tỷ lệ trễ đổi |
Rollback: sửa `current.json` về phiên bản cũ rồi chạy lại `predict`.

## 3. Hợp đồng dữ liệu
`gold.order_predictions` (1 dòng/đơn, partition `months(purchase_date)`)
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| order_id, order_key | STRING, BIGINT | nối với `fact_order_summary` |
| purchase_date | DATE | |
| late_risk | DOUBLE | rủi ro giao trễ 0–1, **để xếp hạng, không phải xác suất** |
| expected_days / safe_promise_days | DOUBLE | số ngày giao dự kiến (trung vị) / hứa giao an toàn (phân vị 92%) |
| is_flagged / is_escalated | INT | top 10% / top 2,5% (chỉ tính trên đơn bán; đơn hủy/không khả dụng luôn = 0) |
| model_version, scored_at | STRING, TIMESTAMP | |

`gold.forecast_daily_orders` (1 dòng/(level × ngày), `level = 'ALL'`)
| Cột | Ý nghĩa |
|---|---|
| origin_date | **ngày cuối cùng đầy đủ** (ngày mới nhất trong dữ liệu có thể chưa đủ nên bị bỏ; ghi đè bằng env `OLIST_COMPLETE_THROUGH=YYYY-MM-DD`) |
| target_date, horizon_day | ngày dự báo, 1–28 |
| forecast, lo80, hi80 | dự báo và khoảng 80% (khoảng chỉ được **nới**, không thu hẹp, để luôn chứa điểm dự báo) |
| model_version, scored_at | |
Ít hơn 60 ngày lịch sử → bảng rỗng (không lỗi). Số đơn thực tế để vẽ nửa trái: đếm `fact_order_summary` (đơn hợp lệ theo `purchase_date`).

**Power BI:** thêm 2 bảng Import từ ClickHouse (cùng mẫu M đang dùng); quan hệ `order_predictions[order_id] → fact_order_summary[order_id]` (1–1) và `forecast_daily_orders[target_date] → dim_date`. Gợi ý: rủi ro trung bình theo bang so với trung bình toàn sàn; bảng đơn cần xử lý (`is_flagged = 1`, sắp theo `late_risk`); đường số đơn thực tế + `forecast` + dải `lo80–hi80`; trang độ tin cậy: nhóm `late_risk` thành 5 mức so với tỷ lệ `is_late` thực tế.

## 4. Trọng số: train, lưu, publish
Train **trong container Spark** để file `.joblib` (pickle) được ghi và đọc bằng cùng một scikit-learn (stage `predict` còn kiểm tra `model_card.json`
và dừng nếu lệch phiên bản).
```
olist-ml\train_model.bat 2018-06-01          :: train, mốc T = 2018-06-01  (~1 phút) → olist-ml\artifacts\
olist-ml\publish_model.bat v1_T2018-06-01    :: tải lên MinIO + trỏ current.json tới phiên bản này
```
Trên MinIO: `lakehouse/models/late_risk/<version>/{late_risk.joblib, forecast_bands.json, thresholds.json, metrics.json, model_card.json}` và
`lakehouse/models/late_risk/current.json` = `{"version": "<version>"}`. Không commit trọng số vào git (`artifacts/` đã được ignore).
Không dùng được `publish_model.bat`? Mở MinIO Console → bucket `lakehouse` → tạo path `models/late_risk/<version>` → upload 5 file trong
`olist-ml\artifacts`, rồi tạo/upload `current.json` vào `models/late_risk/`.

## 5. Chạy lần đầu (theo thứ tự)
1. Build lại image Spark (đã thêm pandas/scikit-learn): `run_lakehouse.bat infra`.
2. `olist-ml\train_model.bat` rồi `olist-ml\publish_model.bat v1_T2018-06-01`.
3. `run_lakehouse.bat all` (hoặc từng stage `silver-facts gold predict clickhouse`).
4. Kiểm chứng (mục 7).
Nếu chưa publish trọng số, `predict` **không làm vỡ pipeline**: in cảnh báo và ghi 2 bảng rỗng đúng schema. Đặt `OLIST_PREDICT_STRICT=true` nếu muốn dừng.
Biến môi trường: `OLIST_MODELS_ROOT` (mặc định `s3a://lakehouse/models/late_risk`), `OLIST_PREDICT_STRICT`, `OLIST_COMPLETE_THROUGH`.

## 6. Lịch train lại
Một Airflow DAG riêng hàng tháng (không nằm trong DAG pipeline hằng ngày): `docker exec … olist_ml.train --train-end <T mới>` →
so `metrics.json` mới với bản đang chạy trên giai đoạn gần nhất → nếu không tệ hơn thì `publish` phiên bản mới. Với mô phỏng thời gian ảo: T là mốc ảo.

## 7. Kiểm chứng sau khi chạy
- `gold.order_predictions` có đúng số đơn có món hàng; `late_risk` ∈ [0,1]; `is_flagged` ≈ 10% đơn bán; chạy hai lần cho kết quả giống nhau.
- `forecast_daily_orders`: 28 dòng, `lo80 ≤ forecast ≤ hi80`, `target_date > origin_date`.
- **Đối chiếu Spark với chấm điểm offline từng đơn** (cách duy nhất kiểm tra phần Spark SQL tạo đặc trưng):
  `python olist-ml\validate_predictions.py --artifacts olist-ml\artifacts --data-dir hosting_server\Data --password <CLICKHOUSE_PASSWORD> --url http://localhost:<cổng HTTP ClickHouse>`
  → dòng cuối phải là `OK: Spark predictions match the offline scoring`.
- Test không cần Spark: `cd olist-ml && python -m pytest -q` (23 test: mô hình, lõi `predict_core`, và test "hợp đồng" kiểm tra Spark/DDL Gold/ClickHouse/Python khớp cột).

## 8. Thay đổi so với repo cũ
Mới: `olist-ml/` (package, test, `train_model.bat`, `publish_model.bat`, `validate_predictions.py`), `layers/predict/`, `docs/ML_PREDICTIONS.md`.
Sửa (thêm, không đổi logic cũ): `olist_pipeline.py` (stage `predict`, import hoãn), `layers/gold/create_gold_tables.sql` (+2 bảng),
`layers/clickhouse/init_gold.sql` và `load_gold.sql` (+2 bảng), `infrastructure/spark/Dockerfile` (python3-pip + pandas/numpy/scikit-learn/joblib),
`infrastructure/docker-compose.yml` (spark-master: mount `olist-ml`, `artifacts`, CSV gốc; `PYTHONPATH`), `run_lakehouse.bat` (stage `predict`).

## 9. Chưa kiểm chứng / hạn chế
- **Chưa chạy trên Spark/Docker thật**: truy vấn Spark tạo đặc trưng, ghi Iceberg, export, `pip install` trong image (tùy phiên bản Python của image),
  nạp ClickHouse và `publish_model.bat`. Đã kiểm bằng test: lõi ML, thứ tự/kiểu cột giữa 4 nơi khai báo, cú pháp. Dùng `validate_predictions.py` để chốt phần Spark.
- Điểm rủi ro mạnh về xếp hạng nhưng tỷ lệ trễ nền thấp nên nhiều cảnh báo sai; chất lượng đổi theo giai đoạn (AUC 0,62–0,77).
- Dữ liệu ~2 năm: không học được mùa vụ năm; Black Friday (1 lần) không dự báo được. Dự báo chỉ ở mức toàn sàn.
