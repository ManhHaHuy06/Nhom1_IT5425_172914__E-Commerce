# Mô tả dữ liệu – Brazilian E-Commerce Public Dataset by Olist

## 1. Tổng quan

Bộ dữ liệu công khai về thương mại điện tử Brazil do **Olist** (sàn kết nối các doanh nghiệp nhỏ với các marketplace tại Brazil) phát hành. Dữ liệu gồm khoảng **100 nghìn đơn hàng** (từ 2016-09 đến 2018-10) được đặt tại nhiều marketplace ở Brazil. Dữ liệu là dữ liệu thương mại thực, đã được ẩn danh; tên cửa hàng/đối tác trong nội dung đánh giá được thay bằng tên các House trong *Game of Thrones*.

Một đơn hàng có thể được nhìn từ nhiều chiều: trạng thái đơn, giá, thanh toán, phí vận chuyển, hiệu quả giao hàng, vị trí khách hàng, thuộc tính sản phẩm và đánh giá của khách.

**Quy trình nghiệp vụ:** khách đặt hàng trên Olist Store → người bán (seller) nhận thông báo và xử lý đơn → giao cho đối tác logistics của Olist → khi khách nhận hàng (hoặc quá ngày giao dự kiến), khách nhận email khảo sát để chấm điểm và viết nhận xét.

**Lưu ý quan trọng**
- Một đơn hàng có thể gồm **nhiều sản phẩm** (nhiều dòng trong `order_items`).
- Mỗi sản phẩm trong cùng một đơn có thể do **một seller khác nhau** đảm nhiệm.
- Một đơn có thể có **nhiều lần thanh toán** (nhiều dòng trong `order_payments`) và đôi khi **nhiều đánh giá**.

## 2. Danh sách bảng

| File | Số dòng | Mô tả | Khóa chính |
|---|---:|---|---|
| `olist_orders_dataset.csv` | 99.441 | Bảng trung tâm: thông tin & mốc thời gian của đơn hàng | `order_id` |
| `olist_order_items_dataset.csv` | 112.650 | Các sản phẩm trong từng đơn (giá, phí vận chuyển, seller) | `order_id` + `order_item_id` |
| `olist_order_payments_dataset.csv` | 103.886 | Các lần thanh toán của đơn | `order_id` + `payment_sequential` |
| `olist_order_reviews_dataset.csv` | 99.224 | Đánh giá của khách sau khi nhận hàng | `review_id` (+ `order_id`) |
| `olist_customers_dataset.csv` | 99.441 | Thông tin khách hàng và vị trí | `customer_id` |
| `olist_sellers_dataset.csv` | 3.095 | Thông tin người bán và vị trí | `seller_id` |
| `olist_products_dataset.csv` | 32.951 | Thuộc tính sản phẩm | `product_id` |
| `olist_geolocation_dataset.csv` | 1.000.163 | Ánh xạ mã zip (5 số đầu) → tọa độ lat/lng | không có (nhiều dòng/zip) |
| `product_category_name_translation.csv` | 71 | Dịch tên danh mục sản phẩm sang tiếng Anh | `product_category_name` |

## 3. Mô tả chi tiết từng bảng

### 3.1 `olist_orders_dataset` – Đơn hàng
| Cột | Mô tả |
|---|---|
| `order_id` | Mã đơn hàng (duy nhất) |
| `customer_id` | Mã khách hàng gắn với đơn (xem lưu ý ở mục 5) |
| `order_status` | Trạng thái đơn: `delivered`, `shipped`, `canceled`, `unavailable`, `invoiced`, `processing`, `created`, `approved` |
| `order_purchase_timestamp` | Thời điểm khách đặt hàng |
| `order_approved_at` | Thời điểm thanh toán được duyệt |
| `order_delivered_carrier_date` | Thời điểm giao cho đơn vị vận chuyển |
| `order_delivered_customer_date` | Thời điểm khách thực nhận hàng |
| `order_estimated_delivery_date` | Ngày giao dự kiến báo cho khách lúc đặt |

Phân bố trạng thái: delivered 96.478 · shipped 1.107 · canceled 625 · unavailable 609 · invoiced 314 · processing 301 · created 5 · approved 2.
Giá trị thiếu: `order_approved_at` (160), `order_delivered_carrier_date` (1.783), `order_delivered_customer_date` (2.965) – chủ yếu là đơn chưa giao/bị hủy.

### 3.2 `olist_order_items_dataset` – Sản phẩm trong đơn
| Cột | Mô tả |
|---|---|
| `order_id` | Mã đơn hàng (FK → orders) |
| `order_item_id` | Số thứ tự sản phẩm trong đơn (1, 2, 3…); cũng là số lượng nếu cùng sản phẩm xuất hiện nhiều lần |
| `product_id` | Mã sản phẩm (FK → products) |
| `seller_id` | Mã người bán (FK → sellers) |
| `shipping_limit_date` | Hạn chót seller phải giao hàng cho đơn vị vận chuyển |
| `price` | Giá của sản phẩm (BRL) |
| `freight_value` | Phí vận chuyển của sản phẩm (BRL). Nếu đơn có nhiều sản phẩm, phí vận chuyển được chia cho các sản phẩm |

### 3.3 `olist_order_payments_dataset` – Thanh toán
| Cột | Mô tả |
|---|---|
| `order_id` | Mã đơn hàng (FK → orders) |
| `payment_sequential` | Thứ tự lần thanh toán (khách có thể trả bằng nhiều phương thức cho một đơn) |
| `payment_type` | Phương thức: `credit_card` (76.795), `boleto` (19.784), `voucher` (5.775), `debit_card` (1.529), `not_defined` (3) |
| `payment_installments` | Số kỳ trả góp |
| `payment_value` | Giá trị giao dịch (BRL) |

### 3.4 `olist_order_reviews_dataset` – Đánh giá
| Cột | Mô tả |
|---|---|
| `review_id` | Mã đánh giá |
| `order_id` | Mã đơn hàng (FK → orders) |
| `review_score` | Điểm hài lòng 1–5 (5: 57.328 · 4: 19.142 · 3: 8.179 · 2: 3.151 · 1: 11.424) |
| `review_comment_title` | Tiêu đề nhận xét (bồ-đào-nha, tùy chọn; thiếu 87.656 dòng) |
| `review_comment_message` | Nội dung nhận xét (tùy chọn; thiếu 58.247 dòng) |
| `review_creation_date` | Ngày gửi khảo sát cho khách |
| `review_answer_timestamp` | Thời điểm khách trả lời khảo sát |

### 3.5 `olist_customers_dataset` – Khách hàng
| Cột | Mô tả |
|---|---|
| `customer_id` | Mã khách **theo từng đơn** (mỗi đơn có một `customer_id` riêng); khóa nối với orders |
| `customer_unique_id` | Mã định danh **thực sự duy nhất** của khách (96.096 khách thực) |
| `customer_zip_code_prefix` | 5 chữ số đầu của mã zip |
| `customer_city` | Thành phố |
| `customer_state` | Bang (mã 2 chữ cái) |

### 3.6 `olist_sellers_dataset` – Người bán
| Cột | Mô tả |
|---|---|
| `seller_id` | Mã người bán |
| `seller_zip_code_prefix` | 5 chữ số đầu mã zip của seller |
| `seller_city` | Thành phố |
| `seller_state` | Bang |

### 3.7 `olist_products_dataset` – Sản phẩm
| Cột | Mô tả |
|---|---|
| `product_id` | Mã sản phẩm |
| `product_category_name` | Danh mục (tiếng Bồ Đào Nha; 73 giá trị khác nhau, thiếu 610 dòng) |
| `product_name_lenght` | Số ký tự của tên sản phẩm *(tên cột gốc viết sai chính tả "lenght")* |
| `product_description_lenght` | Số ký tự của mô tả sản phẩm *(cũng viết sai "lenght")* |
| `product_photos_qty` | Số ảnh của sản phẩm |
| `product_weight_g` | Khối lượng (gram) |
| `product_length_cm` / `product_height_cm` / `product_width_cm` | Dài / cao / rộng (cm) |

Giá trị thiếu: 610 dòng thiếu danh mục, tên, mô tả, số ảnh; 2 dòng thiếu khối lượng và kích thước.

### 3.8 `olist_geolocation_dataset` – Địa lý
| Cột | Mô tả |
|---|---|
| `geolocation_zip_code_prefix` | 5 chữ số đầu mã zip (19.015 giá trị khác nhau) |
| `geolocation_lat` / `geolocation_lng` | Vĩ độ / kinh độ |
| `geolocation_city` | Thành phố |
| `geolocation_state` | Bang |

Mỗi zip prefix có **nhiều dòng** tọa độ (≈1 triệu dòng cho ~19 nghìn prefix), nên cần gộp (ví dụ lấy trung bình lat/lng) trước khi join để tránh nhân đôi dòng.

### 3.9 `product_category_name_translation` – Dịch danh mục
| Cột | Mô tả |
|---|---|
| `product_category_name` | Tên danh mục tiếng Bồ Đào Nha |
| `product_category_name_english` | Tên danh mục tiếng Anh |

File có BOM ở đầu (cột đầu tiên có thể đọc thành `﻿product_category_name`; dùng `encoding='utf-8-sig'` khi đọc).

## 4. Sơ đồ quan hệ giữa các bảng

```
customers ──(customer_id)──► orders ◄──(order_id)── order_items ──(product_id)──► products ──(category)──► category_translation
                               ▲  ▲                      │
                               │  └──(order_id)── order_payments
                               └─────(order_id)── order_reviews
                                                         └──(seller_id)──► sellers

customers.zip_prefix ─┐
sellers.zip_prefix   ─┴──► geolocation.zip_prefix
```

| Join | Khóa |
|---|---|
| orders ↔ customers | `customer_id` |
| orders ↔ order_items / order_payments / order_reviews | `order_id` |
| order_items ↔ products | `product_id` |
| order_items ↔ sellers | `seller_id` |
| products ↔ category_translation | `product_category_name` |
| customers / sellers ↔ geolocation | `*_zip_code_prefix` = `geolocation_zip_code_prefix` |

## 5. Lưu ý khi xử lý dữ liệu

- **`customer_id` ≠ `customer_unique_id`**: `customer_id` được tạo lại cho mỗi đơn (99.441 giá trị), nên muốn đếm số khách thực hoặc phân tích khách quay lại phải dùng `customer_unique_id`.
- **Nhân dòng khi join**: orders → items/payments/reviews là quan hệ 1-nhiều; cộng `price`, `payment_value` sau khi join nhiều bảng sẽ bị tính trùng. Nên tổng hợp từng bảng về cấp `order_id` trước.
- **Giá trị thiếu** tập trung ở các mốc thời gian giao hàng, danh mục sản phẩm, và nội dung nhận xét.
- **Phạm vi thời gian**: đơn hàng từ 2016-09-04 đến 2018-10-17; năm 2016 rất ít dữ liệu và cuối 2018 không đầy đủ.
- **Dữ liệu văn bản** (tên thành phố, danh mục, nhận xét) bằng tiếng Bồ Đào Nha, đã bỏ dấu ở nhiều trường thành phố.
- **Đơn vị tiền tệ**: Real Brazil (BRL).

## 6. Gợi ý hướng phân tích

- **NLP**: phân tích nội dung nhận xét theo nhiều chiều.
- **Phân cụm**: tìm hiểu vì sao khách không viết nhận xét vẫn hài lòng/bực bội.
- **Dự báo doanh số**: dùng thời điểm mua hàng để dự báo doanh thu tương lai.
- **Hiệu quả giao hàng**: so sánh ngày giao thực tế với dự kiến, tối ưu thời gian giao.
- **Chất lượng sản phẩm**: tìm danh mục có tỷ lệ khách không hài lòng cao.
- **Feature engineering**: tạo đặc trưng mới hoặc bổ sung dữ liệu công khai bên ngoài.
- **Marketing**: có thể kết hợp với *Marketing Funnel Dataset* của Olist để nhìn đơn hàng từ góc độ marketing.

## 7. Nguồn

Dữ liệu do **Olist** công bố (www.olist.com), nguồn gốc trên Kaggle: *Brazilian E-Commerce Public Dataset by Olist*.
