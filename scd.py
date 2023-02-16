from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lit,col,row_number

S3_DATA_INPUT_PATH1="s3://scd-11/input/old_file.csv"
S3_DATA_INPUT_PATH2="s3://scd-11/input/new_file.csv"

S3_DATA_OUTPUT_PATH_FILTERED="s3://scd-11/output/final1"

def main():
    spark = SparkSession.builder.appName("Spark_Project").getOrCreate()


    df_old = spark.read.csv(S3_DATA_INPUT_PATH1,\
                            inferSchema=True,header=True)
    df_new = spark.read.csv(S3_DATA_INPUT_PATH2,\
                            inferSchema=True,header=True)

    df_old1 = df_old.withColumn("Start_Date", lit("19-11-2022"))
    df_new1 = df_new.withColumn("Start_Date", lit("20-11-2022"))

    df_union = df_old1.union(df_new1)

    print(f'The total number of records in the source after combining both files {df_union.count()}')

    windowspec = Window.partitionBy("Id").orderBy(col("Start_Date").desc())

    df_type1 = df_union.withColumn("row_num", row_number().over(windowspec)).filter("row_num=1")



    df_type1.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_FILTERED)
    print('The filtered output is uploaded successfully')

if __name__ == '__main__':
    main()
