from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, from_json, window, when, count, min, max, hour, avg, year, current_date, round, datediff, lit, from_unixtime, unix_timestamp
from pyspark.ml import PipelineModel
import os
import toml

class FraudDetectionStreaming:
    def __init__(self, model_path, checkpoint_location="/tmp/spark-checkpoint"):
        self.spark = self._create_spark_session(checkpoint_location)
        # Convert model path to absolute path
        model_path = os.path.abspath(model_path)
        print(f"Loading model from: {model_path}")
        try:
            self.model = PipelineModel.load(model_path)
            print("Model loaded successfully")
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            raise
        self.schemas = self._define_schemas()
        self.kafka_config = self._load_kafka_config()

    def _load_kafka_config(self):
        try:
            # Load Kafka configuration from .streamlit/secrets.toml
            config = toml.load('.streamlit/secrets.toml')
            kafka_config = config['kafka']
            
            # Determine if we're running locally or in the cloud
            is_local = os.environ.get('DEPLOYMENT_ENV', 'local') == 'local'
            bootstrap_servers = kafka_config['local_bootstrap_servers'] if is_local else kafka_config['cloud_bootstrap_servers']
            
            # Basic Kafka configuration
            kafka_params = {
                'kafka.bootstrap.servers': bootstrap_servers,
                'startingOffsets': 'earliest'
            }
            
            # Add security configuration for cloud deployment
            if not is_local and kafka_config['security_protocol'] != "PLAINTEXT":
                kafka_params.update({
                    'kafka.security.protocol': kafka_config['security_protocol'],
                    'kafka.sasl.mechanism': kafka_config['sasl_mechanism'],
                    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
                })
            
            return kafka_params
        except Exception as e:
            print(f"Error loading Kafka configuration: {str(e)}")
            return {'kafka.bootstrap.servers': 'localhost:9092', 'startingOffsets': 'earliest'}

    def _create_spark_session(self, checkpoint_location):
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@23'
        os.environ['SPARK_HOME'] = '/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/pyspark'

        spark = SparkSession.builder \
            .appName("Transaction Streaming Application") \
            .master("local[4]") \
            .config("spark.sql.session.timeZone", "Australia/Melbourne") \
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location) \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()

        # Optimize Spark Configurations
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "10")
        spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

        return spark

    def _define_schemas(self):
        customer_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("username", StringType(), True),
            StructField("email", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("birthdate", TimestampType(), True),
            StructField("first_join_date", TimestampType(), True)])

        browsing_behaviour_schema = StructType([
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("traffic_source", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("ts", IntegerType(), True)])

        product_schema = StructType([
            StructField("id", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("baseColour", StringType(), True),
            StructField("season", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("usage", StringType(), True),
            StructField("productDisplayName", StringType(), True),
            StructField("category_id", StringType(), True)])

        transactions_schema = StructType([
            StructField("created_at", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("product_metadata", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("promo_amount", StringType(), True),
            StructField("promo_code", StringType(), True),
            StructField("shipment_fee", StringType(), True),
            StructField("shipment_location_lat", StringType(), True),
            StructField("shipment_location_long", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("clear_payment", StringType(), True),
            StructField("ts", IntegerType(), True)])

        return {
            'customer': customer_schema,
            'browsing': browsing_behaviour_schema,
            'product': product_schema,
            'transaction': transactions_schema
        }

    def start_streaming(self, customer_file, product_file):
        # Load static data
        customer_df = self.spark.read.csv(customer_file, header=True, schema=self.schemas['customer'])
        customer_df.cache()
        
        product_df = self.spark.read.csv(product_file, header=True, schema=self.schemas['product'])
        product_df.cache()

        # Process customer data
        customer_df = customer_df.withColumn("age", round(datediff(current_date(), col("birthdate")) / 365.25).cast(IntegerType()))
        customer_df = customer_df.withColumn("first_join_year", year(col("first_join_date")))
        customer_df = customer_df.select("customer_id", "gender", "age", "first_join_year")

        # Read streaming data from Kafka
        browsing_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "browsing") \
            .option("startingOffsets", "earliest") \
            .load()

        transaction_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "transaction") \
            .option("startingOffsets", "earliest") \
            .load()

        # Process streaming data
        browsing_stream = self._process_browsing_stream(browsing_df)
        transaction_stream = self._process_transaction_stream(transaction_df)

        # Join streams and make predictions
        predictions_stream = self._join_and_predict(browsing_stream, transaction_stream, customer_df, product_df)

        # Start fraud detection and product analytics queries
        self._start_fraud_detection_query(predictions_stream)
        self._start_product_analytics_query(predictions_stream, product_df)

    def _process_browsing_stream(self, browsing_df):
        array_schema = ArrayType(self.schemas['browsing'])
        browsing_stream = browsing_df.selectExpr("CAST(value AS STRING) as raw_message")
        browsing_stream = browsing_stream \
            .withColumn("json_data", from_json(col("raw_message"), array_schema)) \
            .withColumn("exploded_data", explode(col("json_data"))) \
            .select("exploded_data.*")
        
        browsing_stream = browsing_stream.withColumn("event_ts", col("ts").cast(TimestampType()))
        browsing_stream = browsing_stream.withColumn("event_time", col("event_time").cast(TimestampType()))
        browsing_stream = browsing_stream.withColumn("current_timestamp", unix_timestamp()) \
            .filter(col("current_timestamp") - col("ts") <= 120)
        
        return browsing_stream

    def _process_transaction_stream(self, transaction_df):
        array_schema = ArrayType(self.schemas['transaction'])
        transaction_stream = transaction_df.selectExpr("CAST(value AS STRING) as raw_message")
        transaction_stream = transaction_stream \
            .withColumn("json_data", from_json(col("raw_message"), array_schema)) \
            .withColumn("exploded_data", explode(col("json_data"))) \
            .select("exploded_data.*")
        
        transaction_stream = transaction_stream \
            .select(col("created_at").cast("timestamp"),
                    "customer_id",
                    "transaction_id",
                    "session_id",
                    "product_metadata",
                    "payment_method",
                    "payment_status",
                    col("promo_amount").cast("int"),
                    col("shipment_fee").cast("int"),
                    col("shipment_location_lat").cast("double"),
                    col("shipment_location_long").cast("double"),
                    col("total_amount").cast("int"),
                    "clear_payment",
                    col("ts").cast("int"),
                    from_unixtime(col("ts")).cast("timestamp").alias("event_ts"))
        
        return transaction_stream

    def _join_and_predict(self, browsing_stream, transaction_stream, customer_df, product_df):
        # Apply watermark
        transaction_stream = transaction_stream.withWatermark("event_ts", "30 seconds")
        browsing_stream = browsing_stream.withWatermark("event_ts", "30 seconds")

        # Process session level data
        session_level = browsing_stream.withColumn(
            "event_level", when(col("event_type").isin("AP", "ATC", "CO"), "L1")
                .when(col("event_type").isin("VC", "VP", "VI", "SER"), "L2")
                .otherwise("L3"))

        session_level = session_level.groupBy("session_id", window(col("event_ts"), "30 seconds")) \
            .agg(min("event_time").alias("earliest_time"),
                 max("event_time").alias("latest_time"),
                 count(when(col("event_level") == "L1", True)).alias("L1_count"),
                 count(when(col("event_level") == "L2", True)).alias("L2_count"),
                 count(when(col("event_level") == "L3", True)).alias("L3_count"))

        session_level = session_level.withColumn("L1_ratio", (col("L1_count") / (col("L1_count") + col("L2_count") + col("L3_count"))) * 100) \
            .withColumn("L2_ratio", (col("L2_count") / (col("L1_count") + col("L2_count") + col("L3_count"))) * 100)

        session_level = session_level.withColumn("median_time",
                                                from_unixtime((unix_timestamp("earliest_time") + unix_timestamp("latest_time")) / 2))

        session_level = session_level.withColumn("time_of_day",
                                                when((hour("median_time") >= 6) & (hour("median_time") < 12), "morning")
                                                .when((hour("median_time") >= 12) & (hour("median_time") < 18), "afternoon")
                                                .when((hour("median_time") >= 18) & (hour("median_time") < 24), "evening")
                                                .otherwise("night"))

        session_level = session_level.drop("window", "L1_count", "L2_count", "L3_count", "earliest_time", "latest_time", "median_time")

        # Process purchase data
        purchase_stream = transaction_stream.groupBy("customer_id", "event_ts") \
            .agg(count(when(col("payment_status") == "Success", 1)).alias("num_purchase"))
        purchase_stream = purchase_stream.withColumn("num_purchase", col("num_purchase").cast(IntegerType()))
        purchase_stream = purchase_stream.drop("event_ts")

        # Join all data
        joined_stream = transaction_stream.join(purchase_stream, "customer_id", "inner")
        joined_stream = joined_stream.join(customer_df, "customer_id", "inner")
        joined_stream = joined_stream.join(session_level, "session_id", "inner")
        joined_stream = joined_stream.dropDuplicates(["transaction_id"])
        joined_stream = joined_stream.withColumn("is_fraud", lit(0))

        # Make predictions
        predictions_stream = self.model.transform(joined_stream)
        return predictions_stream

    def _start_fraud_detection_query(self, predictions_stream):
        fraud_window_stream = predictions_stream \
            .filter(col("prediction") == 1.0) \
            .groupBy(window(col("event_ts"), "2 minutes", "10 seconds"),
                    col("transaction_id"),
                    col("shipment_location_lat"),
                    col("shipment_location_long")) \
            .count() \
            .select("window", col("count").alias("fraud_count"),
                    col("transaction_id"),
                    col("shipment_location_lat"),
                    col("shipment_location_long"))

        # Convert to JSON string for Kafka
        fraud_alerts = fraud_window_stream.selectExpr("to_json(struct(*)) AS value")

        fraud_query = fraud_alerts.writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "fraud_alerts") \
            .option("checkpointLocation", "./data/checkpoint/fraud") \
            .start()

        return fraud_query

    def _start_product_analytics_query(self, predictions_stream, product_df):
        product_schema = ArrayType(StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("item_price", IntegerType(), True)]))

        non_fraud_stream = predictions_stream.withColumn("products", from_json(col("product_metadata"), product_schema)) \
            .withColumn("product", explode(col("products"))) \
            .select(col("product.product_id").alias("product_id"),
                    col("product.quantity").alias("quantity"),
                    col("event_ts").alias("event_ts"))

        joined_stream = non_fraud_stream.join(product_df, non_fraud_stream["product_id"] == product_df["id"], how="inner") \
            .select(non_fraud_stream["product_id"],
                    product_df["productDisplayName"],
                    non_fraud_stream["quantity"],
                    non_fraud_stream["event_ts"])

        top_products_stream = joined_stream \
            .groupBy(window(col("event_ts"), "30 seconds", "30 seconds"),
                    col("product_id"),
                    col("productDisplayName")) \
            .agg({"quantity": "sum"}) \
            .withColumnRenamed("sum(quantity)", "total_quantity")

        # Convert to JSON string for Kafka
        top_products = top_products_stream.selectExpr("to_json(struct(*)) AS value")

        top_products_query = top_products.writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "top_products") \
            .option("checkpointLocation", "./data/checkpoint/top_product") \
            .start()

        return top_products_query

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python3 spark_streaming.py <customer_file> <product_file>")
        sys.exit(1)
    
    customer_file = sys.argv[1]
    product_file = sys.argv[2]
    
    app = FraudDetectionStreaming("model/fraud_detection_model_gbt")
    app.start_streaming(customer_file, product_file)
    
    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping the application...")
        sys.exit(0) 