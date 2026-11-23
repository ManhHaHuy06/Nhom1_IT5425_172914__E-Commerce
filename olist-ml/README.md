# olist-ml

Mô hình ML cho Olist: rủi ro giao trễ (+ số ngày giao dự kiến, hứa giao an toàn) và dự báo số đơn 28 ngày. Được stage Spark `predict`
(`olist-lakehouse/layers/predict`) dùng để ghi vào Gold. **Tài liệu đầy đủ: `olist-lakehouse/docs/ML_PREDICTIONS.md`.**

- `olist_ml/predict_core.py` lõi thuần pandas của stage `predict` (đã test); `late_risk.py`, `forecast.py`, `flags.py`, `registry.py`, `train.py`.
- `train_model.bat` train trong container Spark; `publish_model.bat <version>` đưa trọng số lên MinIO; `validate_predictions.py` đối chiếu Spark với offline.
- Test: `pip install -r requirements.txt && python -m pytest -q` (CSV gốc lấy từ `../hosting_server/Data`, đổi bằng `OLIST_DATA_DIR`).
