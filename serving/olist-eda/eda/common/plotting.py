"""Giao diện biểu đồ, định dạng trục và tiện ích pandas cho nhãn tầng / thứ tự nhãn."""
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import seaborn as sns

from .config import REGION_COLORS, REGIONS


# ---------- Tầng / nhãn ----------
def band_labels(edges, fmt=lambda v: f"{v:,.0f}") -> list:
    """Nhãn tầng từ danh sách mốc [lo, hi): "< a", "a–b", …, "≥ z"."""
    labels = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        if lo == edges[0]:
            labels.append(f"< {fmt(hi)}")
        elif np.isinf(hi):
            labels.append(f"≥ {fmt(lo)}")
        else:
            labels.append(f"{fmt(lo)}–{fmt(hi)}")
    return labels


def ordered(df: pd.DataFrame, col: str, order: list, drop_missing: bool = False) -> pd.DataFrame:
    """Sắp xếp các dòng theo thứ tự nhãn cho trước (tầng, phân khúc…); drop_missing bỏ nhãn không có dữ liệu."""
    out = df.set_index(col).reindex(order)
    if drop_missing:
        out = out.dropna(how="all")
    return out.reset_index()


# ---------- Giao diện biểu đồ ----------
def apply_theme():
    sns.set_theme(style="whitegrid", context="notebook")
    plt.rcParams.update({"figure.figsize": (11, 4.5), "axes.titleweight": "bold",
                         "axes.titlesize": 13, "axes.spines.top": False, "axes.spines.right": False,
                         "font.family": "DejaVu Sans"})


def pct_axis(ax, axis="y", decimals=None):
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(mtick.PercentFormatter(1.0, decimals=decimals))


def brl_axis(ax, axis="y", unit="M"):
    div = {"M": 1e6, "K": 1e3}[unit]
    fmt = mtick.FuncFormatter(lambda v, _: f"{v/div:,.1f}{unit}")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def month_axis(ax):
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%y"))
    ax.tick_params(axis="x", rotation=45)


def region_legend(ax, label_fmt: str = "{}", extra: list = (), title: str | None = "Vùng", **kw):
    """Chú thích màu 5 vùng; extra = [(nhãn, màu), ...] thêm vào cuối."""
    items = [(label_fmt.format(r), REGION_COLORS[r]) for r in REGIONS] + list(extra)
    handles = [plt.Rectangle((0, 0), 1, 1, color=color) for _, color in items]
    ax.legend(handles, [label for label, _ in items], title=title, **kw)


def grouped_bar(ax, labels, series: dict, value_fmt: str | None = None):
    """Cột nhóm cạnh nhau; series = {nhãn legend: (giá trị, màu)}; value_fmt (vd. "{:.1%}") ghi giá trị trên cột."""
    x = np.arange(len(labels)); w = 0.8 / len(series)
    for i, (name, (values, color)) in enumerate(series.items()):
        bars = ax.bar(x + (i - (len(series) - 1) / 2) * w, values, w, color=color, label=name)
        if value_fmt:
            ax.bar_label(bars, labels=[value_fmt.format(v) for v in bars.datavalues], padding=2, fontsize=9)
    ax.set_xticks(x, labels); ax.legend()
