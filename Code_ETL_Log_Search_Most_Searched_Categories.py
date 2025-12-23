from datetime import datetime, timedelta
import findspark
findspark.init()

from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.window import Window 
import Code_ETL_Log_Search_Most_Searched_Keyword as KW

spark = SparkSession.builder.getOrCreate()

def read_csv_from_path(path):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df

def rename_Most_Search_column(data, year_month):
    """
    Dynamically rename columns based on the month (e.g., Category_t6, Category_t7).
    """
    year_month = str(int(year_month[4:6] ))
    data = data.withColumnRenamed('Most_Search', 'Most_Searched' + '_t' + year_month)
    data = data.withColumnRenamed('Genre', 'Category_t' + year_month)
    return data

def classify_search_behavior(df):
    """
    Analyze trending behavior by comparing search categories between two periods.
    Identifies if a user's interest has 'Changed' or remained 'Unchanged'.
    """
    df = df.withColumn('Trending_type', 
                       when(col('Category_t6') == col('Category_t7'), 'Unchanged')
                       .otherwise('Changed'))
    df = df.withColumn('Previous', when(col('Trending_type') == 'Changed', 
                                        concat_ws(' - ', col('Category_t6'), col('Category_t7')))
                       .otherwise('Unchanged'))
    return df

def save_data(result, save_path, year_month):
    """Export the behavioral trend analysis to CSV"""
    full_path = f"{save_path}//{year_month}"
    (result.repartition(1)
           .write
           .option("header", "true")
           .mode("overwrite")
           .csv(full_path))
    print(f"Data Saved Successfully to: {full_path}")


def import_to_mysql(df, table_name, mode="overwrite"): 
    """Load the final trend report into MySQL for downstream BI applications"""
    db_url = "jdbc:mysql://localhost:3306/etl_data?useSSL=false&allowPublicKeyRetrieval=true"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df.write.jdbc(
        url=db_url,
        table=table_name,
        mode=mode,          
        properties=properties
    )
    
    print(f"Imported into {table_name} (mode: {mode})")
    print('--------------------------------')

def etl_main(df):
    print('--------------------------------')
    print('Classify Search Behavior')
    print('--------------------------------')
    df = classify_search_behavior(df)

    print('--------------------------------')
    print('Saving Data')
    print('--------------------------------')
    save_data(df, output_path, '202206_202207')

    print('--------------------------------')
    print('Importing to MySQL')
    print('--------------------------------')
    import_to_mysql(df, 'user_behavior_trending', mode='overwrite')
    return df

def main_task_categories(input_path, output_path):
    """
    Main Process: 
    1. Iteratively join monthly datasets based on 'user_id'
    2. Compare month-over-month (MoM) interest changes
    """
    lists = KW.list_files(input_path)
    year_months = KW.convert_to_year_month([], lists)

    print('--------------------------------')
    print('Reading and Merging Data')
    print('--------------------------------')
    df = read_csv_from_path(input_path + year_months[0] + ".csv")
    df = rename_Most_Search_column(df, year_months[0])

    # Perform Inner Join for subsequent months to track returning users
    for i, file_name in enumerate(lists[1:], start=1):
        new_df = read_csv_from_path(input_path + file_name)
        new_df = rename_Most_Search_column(new_df, year_months[i])
        df = df.join(new_df, on='user_id')

    df = etl_main(df)

if __name__ == "__main__":
    # Configure input from AI-processed folder and final output destination
    input_path = r"E:/project/final_project/DataClean/Processed_Categories/"
    output_path = r"E:/project/final_project/DataClean/Log_Search"

    main_task_categories(input_path, output_path)