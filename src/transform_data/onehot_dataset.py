from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import SparkSession
from .create_dataset import aggregate_results_from_date
from pyspark.sql.functions import col, when, lit



# spark = SparkSession.builder \
#     .appName("OneHotEncodingExample") \
#     .getOrCreate()

# spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")


from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

def onehot_encode_spark(df):
    # Xử lý count_lager_2_trips
    df = df.withColumn(
        "count_lager_2_trips_transformed",
        when(col("count_lager_2_trips") == 0, 0)
        .when((col("count_lager_2_trips") >= 1) & (col("count_lager_2_trips") <= 2), 1)
        .when((col("count_lager_2_trips") >= 3) & (col("count_lager_2_trips") <= 5), 2)
        .otherwise(3)
    )

    # Xử lý num_trips_before
    df = df.withColumn(
        "num_trips_before_transformed",
        when(col("num_trips_before") == 0, 0)
        .when(col("num_trips_before") == 1, 1)
        .otherwise(2)
    )

    # Các cột cần one-hot encoding
    columns_to_encode = [
        "pickup_time",
        "pickup_location",
        "top1_pickup_location",
        "top2_pickup_location",
        "top1_drop_location",
        "top2_drop_location",
        "count_lager_2_trips_transformed",
        "num_trips_before_transformed"
    ]
    
    # Tạo StringIndexer và OneHotEncoder
    stages = []
    for col_name in columns_to_encode:
        indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index")
        encoder = OneHotEncoder(inputCol=f"{col_name}_index", outputCol=f"{col_name}_onehot")
        stages += [indexer, encoder]
    
    # Tạo pipeline
    pipeline = Pipeline(stages=stages)
    
    # Áp dụng pipeline
    model = pipeline.fit(df)
    df_transformed = model.transform(df)
    
    # Lấy cột cần thiết
    onehot_columns = [f"{col}_onehot" for col in columns_to_encode]
    result_df = df_transformed.select("is_last_trip", *onehot_columns)
    
    return result_df




def main():
    date = '2024-10-01'
    dataset = aggregate_results_from_date(date)
    dataset_onehot = onehot_encode_spark(dataset)
    dataset_onehot.printSchema()
    dataset_onehot.show()


if __name__ == "__main__":
    # main()

    from .create_dataset import get_daily_dataset

    result = get_daily_dataset('2024-09-30')
    result = onehot_encode_spark(result)

    result.printSchema()
    print(result.count())
    result.show()
 
    # dataset = aggregate_results_from_date('2024-10-01')
