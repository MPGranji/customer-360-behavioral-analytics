# customer-360-behavioral-analytics
ETL xử lý Telecom Logs (JSON/Parquet) bằng PySpark & OpenAI tập trung vào Behavioral Data và Interaction Data.

## 1. Customer 360 là gì?
**Customer 360** là giải pháp xây dựng một cái nhìn toàn diện và thống nhất về khách hàng bằng cách tổng hợp dữ liệu từ tất cả các điểm chạm (touchpoints). Dự án tập trung vào:
* **Hợp nhất dữ liệu:** Kết nối Log nội dung và Log tìm kiếm để tạo hồ sơ khách hàng duy nhất.
* **Thấu hiểu hành vi:** Phân tích mức độ hoạt động (High/Low) và sở thích cá nhân.
* **Phân tích tương tác:** Theo dõi chuyển dịch sở thích tìm kiếm theo thời gian.

## 2. Quy trình thực hiện (Pipeline Process)

![Overall Pipeline Flow](Image_for_readme/overall_pipeline_flow.jpg)

[Image of a data engineering pipeline flowchart showing raw data extraction from JSON and Parquet files, processing with PySpark, AI-driven keyword classification via OpenAI API, and final storage in a MySQL database for Power BI visualization]

Dự án được chia thành hai luồng xử lý độc lập trước khi tổng hợp về kho dữ liệu dùng chung:

### Luồng 1: Xử lý Log Content (Dữ liệu xem nội dung - Tháng 4)
* **Phân loại nội dung:** Chuyển đổi các `AppName` gốc thành các nhóm danh mục: Truyền hình, Phim truyện, Giải trí, Thiếu nhi, Thể thao.
* **Định nghĩa người dùng Active:** Người dùng có từ 15 ngày hoạt động trở lên trong tháng được gắn nhãn **High**, ngược lại là **Low**.
* **Hồ sơ sở thích:** Xác định nội dung xem nhiều nhất (`MostWatch`) và chuỗi sở thích tổng quát (`Taste`).

### Luồng 2: Xử lý Log Search (Dữ liệu tìm kiếm - Tháng 6 & Tháng 7)
* **Trích xuất từ khóa:** Sử dụng Window Function để lọc ra từ khóa có tần suất tìm kiếm cao nhất cho mỗi người dùng hàng tháng.
* **AI Classification:** Tích hợp OpenAI API (`gpt-4o-mini`) để phân loại từ khóa tìm kiếm không cấu trúc thành các thể loại phim/show chuẩn hóa.
* **Phân tích chuyển dịch (Transition Analysis):** So sánh thể loại tìm kiếm giữa Tháng 6 và Tháng 7 để xác định hành vi là `Changed` hoặc `Unchanged`.

## 3. Cấu trúc mã nguồn (Project Structure)

* **[Code_ETL_Log_Content.py](Code_ETL_Log_Content.py)**: Xử lý Log Content, phân loại và tính toán mức độ hoạt động.
* **[Code_ETL_Log_Search_Most_Searched_Keyword.py](Code_ETL_Log_Search_Most_Searched_Keyword.py)**: Trích xuất từ khóa tìm kiếm phổ biến nhất từ định dạng Parquet.
* **[Movie_Classifier.py](Movie_Classifier.py)**: Module AI sử dụng OpenAI để phân loại thể loại nội dung.
* **[Code_ETL_Log_Search_Most_Searched_Categories.py](Code_ETL_Log_Search_Most_Searched_Categories.py)**: Phân tích xu hướng và chuyển dịch hành vi giữa các tháng.

![Project Structure](Image_for_readme/github_repo_structure.jpg)

## 4. Trực quan hóa dữ liệu (Data Visualization)

📊 **[Xem chi tiết báo cáo Power BI tại đây](Customer_360_Analytics.pbix)**

### Tổng quan hành vi (Tháng 4)
* **Quy mô người dùng**: Tổng cộng có **1,920,546 hợp đồng** được phân tích.
* **Mức độ hoạt động**: **71.64%** người dùng thuộc nhóm **High Active**, trong khi **28.36%** thuộc nhóm **Low Active**.
* **Nội dung phổ biến**: "Truyền Hình" là danh mục có lượng tiêu thụ lớn nhất.

![Dashboard Content Overview](Image_for_readme/dashboard_content_overview.jpg)

### Phân tích tìm kiếm & Xu hướng (Tháng 6 - Tháng 7)
* **Sở thích tìm kiếm**: Thể loại **Drama** dẫn đầu lượng tìm kiếm trong cả hai tháng.
* **Biến động hành vi**: Gần **69.13%** người dùng đã thay đổi sở thích tìm kiếm chủ đạo khi bước sang tháng mới (`Changed behavior`).
* **Các luồng chuyển dịch chính**: Người dùng thường xuyên thay đổi giữa **Drama - C Drama**, **Drama - Romance** hoặc **Romance - Drama**.

![Dashboard Search Transitions](Image_for_readme/dashboard_search_behavior.jpg)

## 5. Công nghệ sử dụng (Tech Stack)

* **Ngôn ngữ:** Python.
* **Xử lý dữ liệu:** PySpark (Spark SQL, Window Functions).
* **AI & NLP:** OpenAI API (GPT-4o-mini).
* **Phân tích & Trực quan hóa:** Power BI.
* **Lưu trữ:** MySQL (qua JDBC), CSV.