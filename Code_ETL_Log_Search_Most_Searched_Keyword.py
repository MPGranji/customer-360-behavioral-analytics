from datetime import datetime, timedelta
import findspark
findspark.init()
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.window import Window 
from Code_ETL_Log_Content import list_files

spark = SparkSession.builder.getOrCreate()

def convert_to_year_month(year_months, list_files):
    year_months = sorted(list(set([file[0:6] for file in list_files])))
    return year_months

def read_parquet_from_path(path):
    data = spark.read.parquet(path)
    return data

def process_log_search_data(data):
    """
    Core Logic: Identify the most searched keyword for each user.
    Uses Window Function to rank keywords by search frequency.
    """
    data = data.select('user_id', 'keyword')
    data = data.groupBy('user_id', 'keyword').count()
    data = data.orderBy(col('user_id').desc())
    data = data.withColumnRenamed('count', 'TotalSearch')
    window = Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    data = data.withColumn('Rank', row_number().over(window))
    data = data.filter(col('Rank') == 1)
    data = data.withColumnRenamed('keyword', 'Most_Search')
    data = data.select('user_id', 'Most_Search')
    return data

def save_data(result, save_path, year_month):
    """Export processed results to CSV, partitioned by Year-Month"""
    full_path = f"{save_path}//{year_month}"
    (result.repartition(1)
           .write
           .option("header", "true")
           .mode("overwrite")
           .csv(full_path))
    print(f"Data Saved Successfully to: {full_path}")


def main_task(input_path, output_path):
    """
    Orchestrate the ETL process: 
    1. Group files by Month 
    2. Union daily data 
    3. Analyze search behavior 
    4. Save results
    """
    lists = list_files(input_path)
    # lists.sort()
    year_months = convert_to_year_month([], lists)
    year_month = year_months[0]

    # Process data batch by batch (Month by Month)
    for year_month in year_months:
        print('--------------------------------')
        print('Processing for Year-Month: ', year_month)
        print('--------------------------------')

        # Filter files belonging to the current processing month
        files_in_month = [file for file in lists if file.startswith(year_month)]

        print("ETL_TASK " + input_path + files_in_month[0] + ".parquet")
        # Initial Load
        df = read_parquet_from_path(input_path + files_in_month[0])

        # Iteratively union all daily files within the month
        for file in files_in_month[1:]:
            print("ETL_TASK " + input_path + file + ".parquet")
            new_df = read_parquet_from_path(input_path + file)
            print("Union df with new df")
            df = df.union(new_df)

        print('--------------------------------')
        print('Process log search data ', year_month)
        # Transformation: Find top search keyword per user
        new_result = process_log_search_data(df)
        print('--------------------------------')
        print('Saving csv output for ', year_month)
        # Load: Save to output directory
        save_data(new_result, output_path)
        print('--------------------------------')

if __name__ == "__main__":
    # Path configurations
    input_path = "E://Dataset//log_search//"
    output_path = "E://project//final_project//DataClean//Log_Search"
    main_task(input_path, output_path)