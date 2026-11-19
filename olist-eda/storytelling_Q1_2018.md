# Storytelling: "30 phút trước cuộc họp HĐQT" (Q1/2018)

> **Nhân vật:** CEO một sàn thương mại điện tử (dữ liệu Olist, Brazil).
> **Sự kiện:** Họp Hội đồng quản trị ngày **31/03/2018**, review Q1/2018 (01/01 → 31/03/2018) và chốt kế hoạch cho Q2/2018.
> **Câu hỏi HĐQT cần trả lời:** (1) Dồn ngân sách vào khu vực nào? (2) Đẩy ngành hàng nào? (3) Nhắm nhóm khách hàng nào, họ mua sắm ra sao, có duyệt chương trình thu hút/chăm sóc khách không? (4) Thu hút thêm nhóm người bán nào?
>
> Mọi con số trong tài liệu được truy vấn từ tầng Gold (`iceberg.gold.*`) với kỳ **01/01–31/03/2018**. So sánh **YoY** là với Q1/2017, **QoQ** là với Q4/2017. Đơn vị tiền: BRL. Các mức ngân sách và chỉ tiêu ở phần hành động là **đề xuất minh họa** để HĐQT biểu quyết, không phải số liệu.

---

## 0. Nỗi đau: trước và sau khi có hệ thống

**Trước đây** (cách làm thủ công), khoảng 1–2 tuần trước mỗi cuộc họp:

| Bước | Ai làm | Thời gian ước tính |
|---|---|---|
| Xin dữ liệu đơn hàng, thanh toán, đánh giá từ các bộ phận (IT, CSKH, vận hành) | 1–2 nhân viên | 1–2 ngày (chờ phản hồi) |
| Gộp nhiều file Excel, xử lý trùng lặp, sửa định dạng ngày, ghép mã khách hàng | 1 nhân viên phân tích | 1–2 ngày |
| Tính GMV, tăng trưởng, tỷ lệ giao trễ, phân khúc khách; đối chiếu khi số liệu lệch nhau | 1 nhân viên phân tích | 1 ngày |
| Vẽ biểu đồ, làm slide, sửa theo góp ý của CEO | 1 nhân viên + CEO | 1 ngày |
| **Tổng** | **2–3 người** | **≈ 4–6 ngày công / quý**, số liệu đã cũ ngay khi trình bày |

Ngoài thời gian, cách làm cũ còn ba vấn đề: mỗi phòng ban báo cáo một con số GMV khác nhau, không thể hỏi tiếp "vì sao" ngay trong cuộc họp, và các báo cáo chỉ mô tả hiện trạng chứ không chỉ ra việc cần làm.

**Bây giờ:** dữ liệu tự chảy từ hệ thống bán hàng (OLTP), Airflow trích xuất 15 phút/lần, sau đó qua Lakehouse (Bronze, Silver, Gold), ClickHouse và Power BI. Định nghĩa chỉ số (đơn hợp lệ, GMV, giao trễ, phân khúc RFM…) được tính một lần ở tầng Gold, nên mọi trang dùng cùng một con số. CEO chỉ cần **15–30 phút** đọc 5 trang dashboard theo lộ trình bên dưới, và có thể lọc trực tiếp ngay trong cuộc họp khi HĐQT hỏi.

---

## 1. Lộ trình đọc 30 phút

Trên mọi trang, đặt slicer **Month Start = 01/01/2018 → 31/03/2018** (mặc định của báo cáo).

| Phút | Trang | Câu hỏi cần trả lời | Phục vụ quyết định |
|---|---|---|---|
| 0–5 | **Tổng quan kinh doanh** | Quý này tốt hay xấu? Có "đèn đỏ" nào không? | Bối cảnh cho cả 4 |
| 5–12 | **Khu vực** | Tiền đang đến từ đâu, vùng nào đáng đầu tư thêm? | ① Khu vực |
| 12–18 | **Ngành hàng** | Ngành nào tăng nhanh hơn thị trường và đủ lớn? | ② Ngành hàng |
| 18–24 | **Khách hàng** | Khách là ai, mua khi nào, trả tiền thế nào, có quay lại không? | ③ Khách hàng |
| 24–30 | **Người bán** | Nguồn cung thiếu ở đâu, người bán nào kéo chất lượng xuống? | ④ Người bán |

Mỗi phần dưới đây theo cùng một khuôn: **Nhìn vào biểu đồ, thấy hiện trạng gì, kết luận gì, đề xuất hành động gì, và đo lường bằng KPI nào ở quý sau.**

---

## 2. Trang "Tổng quan kinh doanh": quý này tăng trưởng, nhưng có đèn đỏ

### Hiện trạng (thẻ KPI và 3 biểu đồ)

| Chỉ số | Q1/2018 | So sánh |
|---|---|---|
| GMV | **3,23 triệu** | **+15,4% QoQ**, +282% YoY |
| Số đơn hợp lệ | 20.940 | +19,1% QoQ |
| Số khách hàng | 20.516 | |
| Giá trị đơn TB (AOV) | 154,2 | giảm từ 159,1 (Q4/2017) |
| Tỷ lệ giao trễ | **12,9%** | Q1/2017 chỉ 3,8% |
| Điểm đánh giá TB | **3,90** | Q1/2017 là 4,12 |

Biểu đồ **"Doanh thu theo tháng"** và **"Điểm đánh giá theo tháng"** cho thấy GMV mỗi tháng đều quanh mức 1,0–1,15 triệu, nhưng chất lượng giảm liên tục qua từng tháng:

| Tháng | GMV | % giao trễ | % đánh giá tiêu cực (1–2★) | Điểm TB |
|---|---|---|---|---|
| 01/2018 | 1,10 tr | 5,7% | 14,8% | 4,07 |
| 02/2018 | 0,98 tr | 14,2% | 19,9% | 3,86 |
| 03/2018 | 1,15 tr | **19,0%** | **22,4%** | **3,77** |

### Kết luận
Tăng trưởng doanh thu vẫn tốt, nhưng **đang tăng trưởng bằng cách đốt trải nghiệm khách hàng**. Cứ 5 đơn tháng 3 thì có 1 đơn giao trễ. Nếu tiếp tục dồn tiền quảng cáo khi chưa xử lý vận chuyển, chi phí bỏ ra để kéo khách mới sẽ bị lãng phí vì khách bỏ đi sau đơn đầu tiên (xem mục 5).

### Hành động
- Đặt **"Tỷ lệ giao trễ" thành KPI số 1 của Q2**, ngang hàng với GMV. Mục tiêu: **< 8% vào tháng 6/2018**.
- Mọi khoản ngân sách tăng trưởng ở 4 quyết định bên dưới đều gắn với điều kiện chất lượng: vùng/ngành/người bán có tỷ lệ trễ cao thì sửa trước, quảng bá sau.

---

## 3. Quyết định ①: Đẩy ngân sách vào khu vực nào? (trang "Khu vực")

### Hiện trạng
**Bản đồ + biểu đồ Pareto bang** (chọn Chỉ số = GMV, Cấp độ = Bang):

| Vùng | GMV Q1 | Tỷ trọng | QoQ | % phí ship / GMV | % giao trễ | Điểm TB |
|---|---|---|---|---|---|---|
| Sudeste | 2,09 tr | 64,9% | +15,2% | 13,3% | 11,6% | 3,94 |
| Sul | 0,47 tr | 14,6% | **+20,8%** | 15,0% | 12,6% | 3,93 |
| Nordeste | 0,38 tr | 11,8% | +13,4% | 18,6% | **21,2%** | 3,65 |
| Centro-Oeste | 0,20 tr | 6,2% | +7,7% | 16,2% | 13,5% | 3,85 |
| Norte | 0,08 tr | 2,5% | +21,5% | 18,9% | 19,2% | 3,50 |

- **Pareto:** chỉ 7 bang (SP, RJ, MG, RS, PR, BA, SC) đã chiếm **81%** GMV. Riêng SP chiếm 37,5%.
- **Biểu đồ phân tán "Số đơn/100.000 dân × Tăng trưởng YoY"** chia các bang thành 4 nhóm:
  - **SP**: độ phủ cao nhất (19 đơn/100k dân), giao trễ chỉ 6,7%, điểm 4,11. Đây là thị trường lõi, khỏe mạnh.
  - **RJ**: GMV lớn thứ 2 (13%), nhưng **giao trễ 27,3%, điểm chỉ 3,40**. Bang này có nhu cầu nhưng đang bị vận chuyển làm hỏng trải nghiệm.
  - **Sul (PR, SC, RS)**: độ phủ 9,6–11,4 đơn/100k dân, chỉ bằng khoảng một nửa SP, dù thu nhập và hạ tầng tương đương. Tăng QoQ nhanh nhất (+20,8%), PR có giao trễ chỉ 7,9%. Vùng này còn dư địa và vận hành tốt.
  - **Nordeste**: tăng YoY +314%, nhưng độ phủ chỉ 2–5 đơn/100k dân, phí ship 18,6% GMV, giao trễ 21% (CE 35%, AL 33%). Vùng có nhu cầu thật nhưng chi phí phục vụ cao.

### Kết luận
Không nên rải đều ngân sách, cũng không nên dồn hết vào vùng tăng % cao nhất (Nordeste, Norte) vì ở đó mỗi đơn mới đi kèm phí ship cao và rủi ro đánh giá xấu.

### Hành động đề xuất cho Q2 (ngân sách marketing minh họa = 100%)
| Phân bổ | Khu vực | Lý do | Điều kiện / KPI Q2 |
|---|---|---|---|
| **40%** | **Sul (PR, SC, RS)** | Độ phủ còn thấp so với SP, tăng nhanh nhất, chi phí giao vừa phải | Độ phủ +20% (≈ 12 đơn/100k dân), giữ giao trễ < 10% |
| **35%** | **SP, MG** (giữ thị phần) | Đã chiếm ~50% GMV, vận hành tốt | GMV Sudeste +10% QoQ |
| **15%** | **RJ, chỉ sau khi sửa vận chuyển** | Có nhu cầu lớn nhưng trễ 27% | Chuyển tiền quảng cáo RJ sang **đổi/bổ sung hãng vận chuyển**. Chỉ mở lại quảng cáo khi trễ < 12% |
| **10%** | **Nordeste thí điểm (BA, PE)** | Có tiềm năng dài hạn | Đi kèm tuyển người bán tại chỗ (mục 6). Không chạy quảng cáo đại trà ở CE, MA, AL cho tới khi trễ < 15% |
| 0% | Norte, các bang còn lại | Quy mô 2,5%, ship gần 19% GMV | Chỉ giữ hoạt động tự nhiên |

---

## 4. Quyết định ②: Tập trung thúc đẩy ngành hàng nào? (trang "Ngành hàng")

### Hiện trạng
**Ma trận "Tỷ trọng GMV × Tăng trưởng YoY"**, với mốc tăng trưởng thị trường là +282% YoY:

| Ngành | Tỷ trọng | YoY | QoQ | Phí ship | Điểm TB | Nhóm |
|---|---|---|---|---|---|---|
| computers_accessories | 10,7% | +531% | **+79%** | 14,4% | 3,84 | **Ngôi sao**: lớn và tăng nhanh hơn thị trường |
| sports_leisure | 9,9% | +371% | +44% | 13,8% | 3,88 | **Ngôi sao** |
| watches_gifts | 8,8% | +447% | +2% | 8,0% | 3,83 | Ngôi sao, nhưng đi ngang trong quý |
| health_beauty | 9,6% | +304% | +36% | 12,3% | 3,98 | Trụ cột, tăng ngang thị trường |
| bed_bath_table | 8,4% | +350% | +11% | 16,4% | 3,77 | Trụ cột, chất lượng cần theo dõi |
| baby | 3,8% | **+624%** | +63% | 12,1% | 3,74 | **Tiềm năng**: nhỏ nhưng tăng rất nhanh |
| electronics | 2,0% | **+740%** | **+99%** | **26,2%** | 3,94 | Tiềm năng, nhưng phí ship nặng |
| furniture_decor | 6,1% | +149% | +16% | 18,9% | 3,85 | Lớn nhưng **chậm hơn thị trường** |
| garden_tools, toys, perfumery, cool_stuff | 2–4% mỗi ngành | +94% → +164% | **−20% → −61%** | | | **Đang hạ nhiệt** |
| office_furniture | 3,0% | +190% | +63% | 20,9% | **3,39** | Tăng nhưng **điểm đánh giá thấp nhất** |

**Biểu đồ "Phân khúc giá"**: sản phẩm **< 50 BRL** chiếm 35% số sản phẩm bán ra nhưng chỉ 9,6% GMV, và **phí ship bằng 31,8% giá trị đơn**. Phân khúc 100–500 BRL tạo ra 53% GMV với phí ship chỉ 9–14%.

### Kết luận
Có 3 nhóm việc khác nhau: **đẩy** ngành ngôi sao, **ươm** ngành tiềm năng, và **sửa** ngành có vấn đề về chất lượng hoặc phí ship. Đồng thời, Q2 có hai mùa quà tặng lớn ở Brazil là **Ngày của Mẹ (tháng 5)** và **Ngày Lễ tình nhân Brazil (12/6)**, rất hợp với watches_gifts, health_beauty và perfumery.

### Hành động
1. **Đẩy mạnh (60% ngân sách ngành hàng):** computers_accessories, sports_leisure, health_beauty. Chạy banner trang chủ, voucher theo ngành, ưu tiên hiển thị.
2. **Chiến dịch mùa (20%):** watches_gifts, health_beauty và perfumery cho Ngày của Mẹ. Mục tiêu đưa watches_gifts từ +2% QoQ lên **+25% QoQ**.
3. **Ươm (20%):** baby và electronics. Riêng electronics phải **đàm phán phí ship/đóng gói** trước (26% GMV là phí ship), mục tiêu kéo xuống < 18%.
4. **Không quảng bá, sửa trước:** office_furniture (điểm 3,39): rà soát người bán và quy trình đóng gói hàng cồng kềnh.
5. **Sản phẩm < 50 BRL:** áp dụng **ngưỡng miễn phí ship / mua kèm (combo)** để tăng giá trị giỏ hàng, mục tiêu giảm tỷ lệ phí ship của phân khúc này từ 31,8% xuống < 25%.

---

## 5. Quyết định ③: Nhắm nhóm khách hàng nào? Có phê duyệt chương trình thu hút và chăm sóc không? (trang "Khách hàng")

### Hiện trạng
- **Thẻ "Tỷ lệ khách mua lại":** trong Q1 chỉ **706 trên 20.516 khách (3,4%)** đặt từ 2 đơn trở lên. Tính trên toàn bộ tập khách tới 31/03/2018 (63.420 người), chỉ **3%** từng mua lần thứ hai.
- Tập khách mới mua một lần nhưng có giá trị lớn:
  - **19.810 khách mua lần đầu trong Q1/2018**, giá trị TB 155/khách.
  - **16.608 khách mua lần đầu ở Q4/2017** (đợt Black Friday), chưa quay lại, giá trị TB 160/khách.
- **Biểu đồ "Khoảng thời gian từ đơn đầu tiên tới đơn thứ hai":** trong số những người có quay lại, **51% quay lại trong 30 ngày** và **68% trong 90 ngày**. Sau 90 ngày, khả năng quay lại rất thấp.
- **Khách mua khi nào (ma trận Thứ × Giờ):** Thứ 2 đến Thứ 4 mỗi ngày ~16% số đơn, cuối tuần chỉ 11,6%. Khung **10h–17h chiếm 46%**, 17h–22h chiếm 30%.
- **Khách trả tiền thế nào:** 76% dùng thẻ tín dụng, và phần lớn trả góp. AOV tăng mạnh theo số kỳ: trả 1 lần là 97 BRL, 4 kỳ là 170 BRL, từ 10 kỳ trở lên là **390 BRL**.
- **Biểu đồ "% giao trễ và % đánh giá tiêu cực theo tháng"** cho thấy hai đường đi cùng nhau:

| Thời điểm giao so với cam kết | Điểm TB | % đánh giá 1–2★ |
|---|---|---|
| Sớm từ 10 ngày trở lên | 4,28 | 9,7% |
| Đúng hạn hoặc sớm 0–4 ngày | 3,92 | 14,9% |
| Trễ 1–3 ngày | 3,12 | 36,2% |
| Trễ 4–7 ngày | **1,85** | **74,7%** |
| Trễ > 7 ngày | ~1,5 | 81–85% |

  Đơn giao trễ chỉ chiếm 13% số đơn nhưng tạo ra **gần một nửa số đánh giá tiêu cực** của quý.

### Kết luận
Vấn đề lớn nhất không phải thiếu khách mới mà là **khách mua một lần rồi đi**. Sàn đang có hơn 36.000 khách mới (Q4 và Q1) còn nằm trong "cửa sổ vàng 90 ngày". Chỉ cần kéo được một phần nhỏ quay lại đã có hiệu quả cao hơn so với chi tiền mua khách mới.

### Hành động: đề xuất HĐQT **phê duyệt** 3 chương trình
| # | Chương trình | Nhóm mục tiêu | Cách làm | Ước tính hiệu quả |
|---|---|---|---|---|
| 1 | **"Đơn thứ hai"** (thúc đẩy mua lại) | 36.418 khách mua lần đầu ở Q4/2017 và Q1/2018 | Email/push vào ngày 7, 30, 60 sau đơn đầu. Voucher 15 BRL cho đơn ≥ 100 BRL. Gửi **sáng Thứ 2–4, khung 10h–12h** | Tỷ lệ mua lại 90 ngày từ ~2% lên 4%: thêm ~730 đơn × 154 ≈ **+110 nghìn GMV**; chi phí voucher ≈ 22 nghìn |
| 2 | **"Xin lỗi chủ động"** (chăm sóc khách) | ~2.650 khách nhận đơn trễ trong Q1 | Tự động gửi lời xin lỗi kèm voucher khi đơn trễ hạn, *trước khi* khách viết đánh giá | Giảm % đánh giá 1–2★ của đơn trễ từ ~70% xuống ~50% |
| 3 | **Trả góp 0% cho giỏ hàng ≥ 300 BRL** | Khách thẻ tín dụng mua ngành giá trị cao (computers, electronics, furniture) | Hợp tác ngân hàng, chạy cùng chiến dịch ngành ở mục 4 | AOV từ 154 lên ≥ 160 |

**Không đề xuất trong Q2:** chương trình thành viên VIP. Nhóm khách mua thường xuyên hiện chỉ khoảng 3% tập khách, quy mô quá nhỏ để chương trình hoàn vốn.

**KPI theo dõi Q2:** tỷ lệ khách mua lại ≥ 5%; AOV ≥ 160; điểm TB ≥ 4,0.

---

## 6. Quyết định ④: Mở rộng thu hút nhóm người bán nào? (trang "Người bán")

### Hiện trạng
- **Thẻ "Chênh lệch cung – cầu" + bảng "Vùng × Ngành":**

| Vùng | % nhu cầu (khách mua) | % nguồn cung (người bán) | Số người bán | % đơn phải giao liên vùng |
|---|---|---|---|---|
| Sudeste | 65,8% | 78,8% | 1.006 | 16% |
| Sul | 14,5% | 17,8% | 288 | 78% |
| Nordeste | **11,2%** | **2,0%** | **28** | **96%** |
| Centro-Oeste | 6,1% | 1,4% | 46 | 97% |
| Norte | 2,4% | 0,0% | 2 | 100% |

  Ở Nordeste, các ngành bán chạy nhất gần như không có người bán tại chỗ: health_beauty (9% cung nội vùng), watches_gifts (1%), sports_leisure (2%), auto (2%), cool_stuff, furniture_decor, bed_bath_table (0%). Đây chính là lý do phí ship 18,6% và giao trễ 21% ở mục 3.
- **Biểu đồ "Tầng người bán":** 10% người bán hàng đầu (137 người) tạo ra **59,7% GMV**, trong khi 50% người bán dưới cùng (685 người) chỉ tạo ra 6,2%.
- **Biểu đồ "GMV theo cohort":** 369 người bán **mới gia nhập trong Q1/2018** đã đóng góp **13,2% GMV**. Kênh tuyển người bán đang hoạt động hiệu quả.
- **Biểu đồ phân tán "Tỷ lệ đúng hạn × Điểm đánh giá"** (người bán có ≥ 10 đơn trong quý):
  - **135 người bán "vùng đỏ"** (điểm < 3,5 hoặc trễ > 20%) nắm **26,3% GMV**, trễ TB 22,5%.
  - **81 người bán "ngôi sao"** (điểm ≥ 4,2 và trễ ≤ 8%) chỉ nắm 8,6% GMV.

### Kết luận
Câu hỏi không phải là "tuyển thêm bao nhiêu người bán", mà là **tuyển ai, ở đâu**, và **dồn lưu lượng cho ai**. Tuyển thêm người bán đại trà ở Sudeste chỉ làm tăng cạnh tranh nội bộ, không giải quyết được giao trễ.

### Hành động
1. **Tuyển người bán tại Nordeste (BA, PE, CE):** mục tiêu **từ 28 lên 80 người bán** trong Q2, ưu tiên health_beauty, watches_gifts, sports_leisure, bed_bath_table, furniture_decor. Ưu đãi: miễn/giảm hoa hồng 3 tháng đầu, hỗ trợ chụp ảnh và đăng sản phẩm. KPI: tỷ lệ giao liên vùng của Nordeste từ 96% xuống < 85%.
2. **Tuyển người bán theo ngành tăng trưởng** (computers_accessories, baby, electronics), ưu tiên người bán ở Sul và Sudeste có kho gần khách.
3. **Chương trình chất lượng người bán:** 135 người bán vùng đỏ nhận kế hoạch cải thiện 30 ngày (SLA giao hàng, đóng gói). Sau 30 ngày chưa đạt thì giảm thứ hạng hiển thị. Đồng thời gắn huy hiệu và tặng tín dụng quảng cáo cho 81 người bán ngôi sao. Mục tiêu: tỷ trọng GMV của người bán ngôi sao từ 8,6% lên **15%**.
4. **Giữ chân top 10%:** chỉ định account manager riêng, vì 137 người này nắm gần 60% GMV. Mất 1–2 người bán lớn sẽ ảnh hưởng trực tiếp tới doanh thu.

---

## 7. Tờ trình 1 trang cho HĐQT (bản chốt cuối buổi họp)

| Quyết định | Đề xuất cho Q2/2018 | Bằng chứng chính (Q1/2018) | KPI kiểm tra cuối Q2 |
|---|---|---|---|
| **Điều kiện tiên quyết** | Xử lý giao trễ là ưu tiên số 1 | Trễ tăng từ 5,7% lên 19,0% trong 3 tháng; đơn trễ gây ~50% đánh giá xấu | Giao trễ < 8%, điểm TB ≥ 4,0 |
| ① Khu vực | 40% Sul, 35% SP+MG, 15% RJ (dùng để sửa vận chuyển), 10% thí điểm BA+PE | Sul tăng +20,8% QoQ, độ phủ chỉ bằng ½ SP; RJ trễ 27% | Độ phủ Sul ≥ 12 đơn/100k dân; RJ trễ < 12% |
| ② Ngành hàng | Đẩy computers_accessories, sports_leisure, health_beauty; chiến dịch Ngày của Mẹ cho watches_gifts; ươm baby, electronics; tạm dừng quảng bá office_furniture | computers_accessories +79% QoQ; watches_gifts đi ngang; office_furniture điểm 3,39 | watches_gifts +25% QoQ; phí ship electronics < 18% |
| ③ Khách hàng | **Phê duyệt** "Đơn thứ hai", "Xin lỗi chủ động", trả góp 0% cho giỏ ≥ 300 | Chỉ 3% khách mua lại; 68% lần quay lại diễn ra trong 90 ngày; 36 nghìn khách mới còn trong cửa sổ này | Tỷ lệ mua lại ≥ 5%; AOV ≥ 160 |
| ④ Người bán | Tuyển người bán tại Nordeste (28 lên 80), tuyển theo ngành tăng trưởng, chương trình chất lượng cho 135 người bán vùng đỏ | Nordeste chiếm 11% nhu cầu nhưng chỉ 2% nguồn cung; 135 người bán vùng đỏ nắm 26% GMV | Giao liên vùng Nordeste < 85%; người bán ngôi sao ≥ 15% GMV |

**Cuộc họp cuối Q2 (30/06/2018):** CEO mở lại đúng 5 trang này, đổi slicer sang 01/04–30/06/2018 và đối chiếu từng KPI trong cột cuối. Không ai phải làm lại báo cáo từ đầu.

---

## 8. Kịch bản nói mẫu (khoảng 2 phút mở đầu cuộc họp)

> "Q1 chúng ta đạt 3,23 triệu GMV, tăng 15% so với quý trước và gần gấp 4 so với cùng kỳ. Nhưng tôi muốn HĐQT nhìn vào đường màu đỏ này: tỷ lệ giao trễ tăng từ 6% lên 19% chỉ trong 3 tháng, và điểm đánh giá giảm theo đúng nhịp đó. Đơn trễ trên 4 ngày thì 3/4 khách cho 1–2 sao.
>
> Vì vậy cả 4 đề xuất hôm nay đều xoay quanh một nguyên tắc: **tăng trưởng ở nơi chúng ta phục vụ tốt, sửa trước ở nơi chúng ta phục vụ kém.** Cụ thể: dồn ngân sách vào Sul, nơi độ phủ chỉ bằng một nửa São Paulo nhưng giao hàng tốt. Đẩy ba ngành máy tính, thể thao, làm đẹp, và chạy chiến dịch Ngày của Mẹ. Phê duyệt chương trình 'Đơn thứ hai' cho 36 nghìn khách mới đang nằm trong cửa sổ 90 ngày. Và tuyển người bán ngay tại Nordeste thay vì tiếp tục ship hàng xuyên đất nước."

---

## Phụ lục: lưu ý khi trình bày bằng dashboard thật

- Các số trên được truy vấn trực tiếp từ Gold (Trino) ngày 18/11/2026. Trên Power BI, các thẻ "(Kỳ này)" sẽ cho cùng con số khi slicer đặt 01/01–31/03/2018. Sai khác nhỏ có thể xuất hiện do cách làm tròn hoặc measure DAX (ví dụ "Tỷ lệ khách mua lại" tính theo định nghĩa trong model).
- Bản build Gold hiện tại có `report_period` = 01–08/2018. Vì vậy các cột tính sẵn trong Gold (`dim_customer_profile.segment` RFM, `dim_state.quadrant`, `dim_category.quadrant`, `dim_seller.seller_tier`, `cust_2nd_gap`) được tính trên **toàn bộ dữ liệu tới 08/2018**, không thay đổi theo slicer. Khi kể chuyện "họp ngày 31/03/2018", nên dùng các measure "(Kỳ này)" và các số trong tài liệu này, không đọc trực tiếp phân khúc RFM trên trang Tổng quan.
- ClickHouse `olist_gold` hiện đang rỗng (0 dòng). Trước khi mở Power BI cần chạy lại bước nạp:
  ```powershell
  .\run_lakehouse.bat clickhouse
  ```
