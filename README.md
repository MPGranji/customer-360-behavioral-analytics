# customer-360-behavioral-analytics
ETL xử lý Telecom Logs (JSON/Parquet) bằng PySpark & OpenAI tập trung vào Behavioral Data và Interaction Data 

## 1. Customer 360 là gì?
**Customer 360** là giải pháp xây dựng một cái nhìn toàn diện và thống nhất về khách hàng bằng cách tổng hợp dữ liệu từ tất cả các điểm chạm (touchpoints) và nguồn tương tác khác nhau trong hệ thống.

Trong dự án này, Customer 360 tập trung vào:
* **Hợp nhất dữ liệu (Data Unification):** Kết nối các nguồn dữ liệu rời rạc từ Log nội dung và Log tìm kiếm để tạo ra một hồ sơ khách hàng duy nhất.
* **Thấu hiểu hành vi (Behavioral Insights):** Phân tích sâu các chỉ số về mức độ hoạt động (Active/Low), thời lượng xem, và sở thích cá nhân của từng hợp đồng.
* **Phân tích tương tác (Interaction Data):** Theo dõi cách người dùng tìm kiếm và chuyển dịch sở thích theo thời gian để tối ưu hóa trải nghiệm người dùng.

## 2. Quy trình thực hiện (Pipeline Process)

![Overall Pipeline Flow](./image_for_readme/overall_pipeline_flow.jpg)

Dự án được chia thành hai luồng xử lý độc lập trước khi tổng hợp về kho dữ liệu dùng chung:

### Luồng 1: Xử lý Log Content (Dữ liệu xem nội dung - Tháng 4)
* **Phân loại nội dung:** Chuyển đổi các `AppName` gốc thành các nhóm danh mục: Truyền hình, Phim truyện, Giải trí, Thiếu nhi, Thể thao.
* **Định nghĩa người dùng Active:** Người dùng có từ 15 ngày hoạt động trở lên trong tháng được gắn nhãn **High**, ngược lại là **Low**.
* **Hồ sơ sở thích:** Xác định nội dung xem nhiều nhất (`MostWatch`) và chuỗi sở thích tổng quát (`Taste`) dựa trên thời lượng tiêu thụ.

### Luồng 2: Xử lý Log Search (Dữ liệu tìm kiếm - Tháng 6 & Tháng 7)
* **Trích xuất từ khóa:** Sử dụng kỹ thuật Window Function để lọc ra từ khóa có tần suất tìm kiếm cao nhất cho mỗi `user_id` hàng tháng.
* **AI Classification:** Tích hợp OpenAI API (`gpt-4o-mini`) kết hợp Multithreading để phân loại từ khóa tìm kiếm không cấu trúc thành các thể loại phim/show chuẩn hóa.
* **Phân tích chuyển dịch (Transition Analysis):** So sánh thể loại tìm kiếm chủ đạo giữa Tháng 6 và Tháng 7 để xác định hành vi là `Changed` hoặc `Unchanged`.

## 3. Cấu trúc mã nguồn (Project Structure)

* **[Code_ETL_Log_Content.py](./Code_ETL_Log_Content.py)**: Xử lý Log Content, phân loại và tính toán mức độ hoạt động.
* **[Code_ETL_Log_Search_Most_Searched_Keyword.py](./Code_ETL_Log_Search_Most_Searched_Keyword.py)**: Trích xuất từ khóa tìm kiếm phổ biến nhất từ định dạng Parquet.
* **[Movie_Classifier.py](./Movie_Classifier.py)**: Module AI sử dụng OpenAI để phân loại thể loại nội dung từ dữ liệu tìm kiếm.
* **[Code_ETL_Log_Search_Most_Searched_Categories.py](./Code_ETL_Log_Search_Most_Searched_Categories.py)**: Phân tích xu hướng và chuyển dịch hành vi giữa các tháng.

## 4. Trực quan hóa dữ liệu (Data Visualization)

📊 **[Xem chi tiết báo cáo Power BI tại đây](./Customer_360_Analytics.pbix)**

Dữ liệu sau khi xử lý được trực quan hóa để phục vụ việc ra quyết định kinh doanh:

### Tổng quan hành vi (Tháng 4)
* **Quy mô người dùng**: Tổng cộng có **1,920,546 hợp đồng** được phân tích.
* **Mức độ hoạt động**: **71.64%** (1.38M) người dùng thuộc nhóm **High Active**, trong khi **28.36%** (0.54M) thuộc nhóm **Low Active**.
* **Nội dung phổ biến**: "Truyền Hình" là danh mục có lượng tiêu thụ lớn nhất.

![Dashboard Content Overview](./image_for_readme/dashboard_content_overview.jpg)

### Phân tích tìm kiếm & Xu hướng (Tháng 6 - Tháng 7)
* **Sở thích tìm kiếm**: Thể loại **Drama** dẫn đầu lượng tìm kiếm trong cả hai tháng, tiếp theo là **C Drama** và **Animation**.
* **Biến động hành vi**: Gần **69.13%** người dùng đã thay đổi sở thích tìm kiếm chủ đạo khi bước sang tháng mới (`Changed behavior`).
* **Các luồng chuyển dịch chính**: Người dùng thường xuyên thay đổi giữa **Drama - C Drama**, **Drama - Romance** hoặc **Romance - Drama**.

![Dashboard Search Transitions](./image_for_readme/dashboard_search_behavior.jpg)

## 5. Công nghệ sử dụng (Tech Stack)

* **Ngôn ngữ:** Python.
* **Xử lý dữ liệu:** PySpark (Spark SQL, Window Functions).
* **AI & NLP:** OpenAI API (GPT-4o-mini).
* **Lưu trữ:** MySQL (qua JDBC), CSV.
* **Phân tích & Trực quan hóa:** Power BI