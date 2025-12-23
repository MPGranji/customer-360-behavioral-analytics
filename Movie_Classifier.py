import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import findspark
from dotenv import load_dotenv
from openai import OpenAI

findspark.init()
from pyspark.sql import SparkSession

# Load environment variables and initialize global clients
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

def load_data_from_spark(path):
    print(f"Reading data from: {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    return df.toPandas()

def get_unique_keywords(df, column_name):
    unique_list = df[column_name].dropna().unique().tolist()
    print(f"Total rows: {len(df)} | Unique keywords: {len(unique_list)}")
    return unique_list

def save_to_csv(df, path):
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
    
    print(f"Saving results to: {path}")
    df.to_csv(path, index=False) 
    print("File saved successfully!")

# AI CLASSIFICATION LOGIC
def classify_batch(movie_list):
    """Send a batch of movie titles to AI for classification"""
    if not movie_list:
        return {}

    prompt = f"""
    Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí.  
    Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan đến nội dung.

    ⚠️ Nguyên tắc quan trọng:
    - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa.  
    - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng.  
    - Nếu không chắc → chọn thể loại gần nhất (VD: từ mô tả tình cảm → Romance, tên địa danh thể thao → Sports, chương trình giải trí → Reality Show, v.v.)

    Nhiệm vụ của bạn:
    1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả (vd: "thuyếtminh" → "Thuyết minh", "tramnamu" → "Trăm năm hữu duyên", "capdoi" → "Cặp đôi").
    2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
    - Tên phim, series, show, chương trình
    - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
    - Từ khoá mô tả nội dung (→ phân loại theo ý nghĩa, ví dụ "thuyếtminh" → "Other" hoặc "Drama", "bigfoot" → "Horror")
    3. **Gán thể loại phù hợp nhất** trong các nhóm sau:  
    - Action  
    - Romance  
    - Comedy  
    - Horror  
    - Animation  
    - Drama  
    - C Drama  
    - K Drama  
    - Sports  
    - Music  
    - Reality Show  
    - TV Channel  
    - News  
    - Other

    Một số quy tắc gợi ý nhanh:
    - Có từ “VTV”, “HTV”, “Channel” → TV Channel  
    - Có “running”, “master key”, “reality” → Reality Show  
    - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News  
    - “sex”, “romantic”, “love” → Romance  
    - “potter”, “hogwarts” → Drama / Fantasy  
    - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

    Chỉ trả về **1 JSON object**.  
    Key = tên gốc trong danh sách.  
    Value = thể loại đã phân loại.

    Ví dụ:  
    {{
    "thuyếtminh": "Other",
    "bigfoot": "Horror",
    "capdoi": "Romance",
    "ARGEN": "Sports",
    "nhật ký": "Drama",
    "PENT": "C Drama",
    "running": "Reality Show",
    "VTV3": "TV Channel"
    }}

    Danh sách:
    {movie_list}
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        text = resp.choices[0].message.content.strip()

        # Extract JSON block
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end == -1:
            return {m: "Other" for m in movie_list}
        
        parsed = json.loads(text[start:end + 1])
        return {m: parsed.get(m, "Other") for m in movie_list}

    except Exception as e:
        print(f"Error in batch: {e}")
        return {m: "Other" for m in movie_list}

def run_classification_pipeline(input_path, output_path, workers=30, batch_size=50):
    """Orchestrate the ETL process"""
    
    # Extract
    full_pdf = load_data_from_spark(input_path)
    
    # Transform (Prepare unique list and Batching)
    unique_movies = get_unique_keywords(full_pdf, "Most_Search")
    chunks = [unique_movies[i:i + batch_size] for i in range(0, len(unique_movies), batch_size)]
    all_mappings = {}

    # Transform (Multithreaded AI Classification)
    print(f"Starting multithreaded processing with {workers} workers")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_batch = {executor.submit(classify_batch, chunk): chunk for chunk in chunks}
        
        completed = 0
        for future in as_completed(future_to_batch):
            all_mappings.update(future.result())
            completed += 1
            if completed % 10 == 0 or completed == len(chunks):
                print(f"Progress: {completed}/{len(chunks)} batches completed")

    # Finalize Mapping
    print("Mapping results back to main dataframe...")
    full_pdf["Genre"] = full_pdf["Most_Search"].map(lambda x: all_mappings.get(str(x), "Other"))
    
    # Load
    save_to_csv(full_pdf, output_path)
    
    spark.stop()

if __name__ == "__main__":
 
    INPUT_FILE = r"\project\final_project\DataClean\Log_Search\202206"
    OUTPUT_FILE = r"E:\project\final_project\DataClean\Processed_Categories\202206.csv"
    
    run_classification_pipeline(
        input_path=INPUT_FILE, 
        output_path=OUTPUT_FILE, 
        workers=30, 
        batch_size=50
    )