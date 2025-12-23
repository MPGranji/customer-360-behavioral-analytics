# customer-360-behavioral-analytics
ETL xử lý Telecom Logs (JSON/Parquet) bằng PySpark & OpenAI tập trung vào Behavioral Data và Interaction Data.

## 1. Customer 360 là gì?
**Customer 360** là giải pháp xây dựng một cái nhìn toàn diện và thống nhất về khách hàng bằng cách tổng hợp dữ liệu từ tất cả các điểm chạm (touchpoints). Dự án tập trung vào:
* **Hợp nhất dữ liệu:** Kết nối Log nội dung (JSON) và Log tìm kiếm (Parquet) để tạo hồ sơ khách hàng duy nhất.
* **Thấu hiểu hành vi:** Phân tích mức độ hoạt động (High/Low) dựa trên số ngày online và sở thích cá nhân.
* **Phân tích tương tác:** Theo dõi sự chuyển dịch sở thích tìm kiếm theo thời gian.

## 2. Quy trình thực hiện (Pipeline Process)

<p align="center">
  <img src="image_for_readme/overall_pipeline_flow.jpg" width="80%" alt="Overall Pipeline Flow">
</p>



Dự án được chia thành hai luồng xử lý độc lập trước khi tổng hợp về kho dữ liệu dùng chung:

### Luồng 1: Xử lý Log Content (Dữ liệu xem nội dung - Tháng 4)
* **Phân loại nội dung:** Chuyển đổi các `AppName` gốc thành các nhóm: Truyền hình, Phim truyện, Giải trí, Thiếu nhi, Thể thao.
* **Định nghĩa người dùng Active:** Người dùng có từ 15 ngày hoạt động trở lên trong tháng được gắn nhãn **High**, ngược lại là **Low**.
* **Hồ sơ sở thích:** Xác định nội dung xem nhiều nhất (`MostWatch`) và chuỗi sở thích (`Taste`) dựa trên thời lượng tiêu thụ.

### Luồng 2: Xử lý Log Search (Dữ liệu tìm kiếm - Tháng 6 & Tháng 7)
* **Trích xuất từ khóa:** Sử dụng Window Function để lọc từ khóa tìm kiếm cao nhất cho mỗi người dùng hàng tháng.
* **AI Classification:** Tích hợp OpenAI API (`gpt-4o-mini`) để phân loại từ khóa không cấu trúc thành các thể loại phim chuẩn hóa.
* **Phân tích chuyển dịch:** So sánh thể loại tìm kiếm chủ đạo giữa Tháng 6 và Tháng 7 để xác định hành vi là `Changed` hoặc `Unchanged`.

## 3. Cấu trúc mã nguồn (Project Structure)
* **[Code_ETL_Log_Content.py](./Code_ETL_Log_Content.py)**: Xử lý Log Content và tính mức độ hoạt động.
* **[Code_ETL_Log_Search_Most_Searched_Keyword.py](./Code_ETL_Log_Search_Most_Searched_Keyword.py)**: Trích xuất từ khóa phổ biến nhất.
* **[Movie_Classifier.py](./Movie_Classifier.py)**: Phân loại nội dung bằng OpenAI API.
* **[Code_ETL_Log_Search_Most_Searched_Categories.py](./Code_ETL_Log_Search_Most_Searched_Categories.py)**: Phân tích xu hướng và chuyển dịch hành vi.

<p align="center">
  <img src="image_for_readme/github_repo_structure.jpg" width="70%" alt="Project Structure">
</p>

## 4. Trực quan hóa dữ liệu (Data Visualization)
📊 **[Xem chi tiết báo cáo Power BI tại đây](./Customer_360_Analytics.pbix)**

### Tổng quan hành vi (Tháng 4)
* **Quy mô**: Hệ thống phân tích **1,920,546 hợp đồng**.
* **Hoạt động**: Ghi nhận **71.64%** người dùng High Active và **28.36%** Low Active.
* **Nội dung**: "Truyền Hình" là danh mục có lượng tiêu thụ áp đảo.

<p align="center">
  <img src="image_for_readme/dashboard_content_overview.jpg" width="90%" alt="Dashboard Content Overview">
</p>

### Phân tích tìm kiếm & Xu hướng (Tháng 6 - Tháng 7)
* **Sở thích**: **Drama** dẫn đầu lượng tìm kiếm trong cả hai tháng.
* **Biến động**: **69.13%** người dùng đã thay đổi sở thích tìm kiếm chủ đạo (`Changed behavior`).
* **Chuyển dịch**: Xu hướng thay đổi mạnh nhất giữa các cặp **Drama - C Drama** và **Drama - Romance**.

<p align="center">
  <img src="image_for_readme/dashboard_search_behavior.jpg" width="90%" alt="Dashboard Search Transitions">
</p>

## 5. Công nghệ sử dụng (Tech Stack)
* **Ngôn ngữ**: Python.
* **Xử lý dữ liệu**: PySpark (Spark SQL, Window Functions).
* **AI & NLP**: OpenAI API (GPT-4o-mini).
* **Trực quan hóa**: Power BI.
* **Lưu trữ**: MySQL (JDBC), CSV.