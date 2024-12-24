from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from src.transform_data.onehot_dataset import onehot_encode_spark
from src.transform_data.create_dataset import aggregate_results_from_date
from pyspark.sql.functions import col, when
from pyspark.ml.classification import RandomForestClassifier


def train(df, lr_save_path, rf_save_path):
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("TrainAndValidate").getOrCreate()

    # Chia dữ liệu thành train và validation
    train_df, val_df = df.randomSplit([0.8, 0.2], seed=42)

    # Cân bằng dữ liệu
    majority_class = train_df.filter(col("is_last_trip") == 0)
    minority_class = train_df.filter(col("is_last_trip") == 1)

    # Lấy mẫu dữ liệu lớp thiểu số để cân bằng
    minority_oversampled = minority_class.sample(withReplacement=True, fraction=majority_class.count() / minority_class.count())
    balanced_train_df = majority_class.union(minority_oversampled)

    # Danh sách các cột one-hot encoding
    feature_columns = [
        "pickup_time_onehot",
        "pickup_location_onehot",
        "top1_pickup_location_onehot",
        "top2_pickup_location_onehot",
        "top1_drop_location_onehot",
        "top2_drop_location_onehot",
        "count_lager_2_trips_transformed_onehot",
        "num_trips_before_transformed_onehot"
    ]


    # Tạo VectorAssembler để chuyển đổi thành một cột feature vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    train_df = assembler.transform(balanced_train_df)
    val_df = assembler.transform(val_df)

    lr = LogisticRegression(featuresCol="features", labelCol="is_last_trip", predictionCol="prediction", maxIter=10)
    lr_model = lr.fit(train_df)

    lr_val_predictions = lr_model.transform(val_df)
    lr_evaluator = BinaryClassificationEvaluator(labelCol="is_last_trip", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    lr_roc_auc = lr_evaluator.evaluate(lr_val_predictions)
    print(f"Logistic Regression ROC AUC on validation set: {lr_roc_auc:.4f}")
    lr_model.save(lr_save_path)

    print(f"Logistic Regression model saved to {lr_save_path}")

    # Random Forest Model
    rf = RandomForestClassifier(featuresCol="features", labelCol="is_last_trip", predictionCol="prediction", numTrees=50, maxDepth=5)
    rf_model = rf.fit(train_df)

    # Dự đoán với Random Forest
    rf_val_predictions = rf_model.transform(val_df)
    rf_evaluator = BinaryClassificationEvaluator(labelCol="is_last_trip", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    rf_roc_auc = rf_evaluator.evaluate(rf_val_predictions)
    print(f"Random Forest ROC AUC on validation set: {rf_roc_auc:.4f}")
    rf_model.save(rf_save_path)
    print(f"Random Forest model saved to {rf_save_path}")



if __name__=="__main__":
    date = "2024-10-01"
    dataset = aggregate_results_from_date(date)
    dataset_onehot = onehot_encode_spark(dataset)

    lr_model_path = "data/model/logistic_regression_model.model"
    rf_model_path = "data/model/random_forest_model.model"

    train(dataset_onehot, lr_model_path, rf_model_path)


    from pyspark.ml.classification import LogisticRegressionModel, RandomForestClassificationModel
