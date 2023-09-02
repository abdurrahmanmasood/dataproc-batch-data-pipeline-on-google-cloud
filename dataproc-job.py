# Import libraries
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import col, when, dayofmonth, month, year


# Create spark session for your production job
spark = SparkSession.builder.appName("NYC Taxi").getOrCreate()


# Read file and create dataframe
df = spark.read.parquet("gs://data_lake_<name>/yellow_tripdata_{}.parquet".format(date.today()))


# Add columns for day, month, year using datetime field
df = df.withColumn("tpep_pickup_datetime_day", dayofmonth(col("tpep_pickup_datetime")))
df = df.withColumn("tpep_pickup_datetime_month", month(col("tpep_pickup_datetime")))
df = df.withColumn("tpep_pickup_datetime_year", year(col("tpep_pickup_datetime")))
df = df.withColumn("tpep_dropoff_datetime_day", dayofmonth(col("tpep_dropoff_datetime")))
df = df.withColumn("tpep_dropoff_datetime_month", month(col("tpep_dropoff_datetime")))
df = df.withColumn("tpep_dropoff_datetime_year", year(col("tpep_dropoff_datetime")))


# Change values of columns with numeric values to their actual values. This information is provided in data dictionary of this dataset.
df = df.withColumn("VendorID",
    when(col("VendorID") == 1, "Creative Mobile Technologies")
    .when(col("VendorID") == 2, "VeriFone Inc")
    .otherwise(col("VendorID"))
)

df = df.withColumn("RateCodeID",
    when(col("RateCodeID") == 1, "Standard rate")
    .when(col("RateCodeID") == 2, "JFK")
    .when(col("RateCodeID") == 3, "Newark")
    .when(col("RateCodeID") == 4, "Nassau or Westchester")
    .when(col("RateCodeID") == 5, "Negotiated fare")
    .when(col("RateCodeID") == 6, "Group ride")
    .otherwise(col("RateCodeID"))
)

df = df.withColumn("Store_and_fwd_flag",
    when(col("Store_and_fwd_flag") == "Y" ,"store and forward trip")
    .when(col("Store_and_fwd_flag") == "N", "not a store and forward trip")
    .otherwise(col("Store_and_fwd_flag"))
)

df = df.withColumn("Payment_type",
    when(col("Payment_type") == 1, "Credit card")
    .when(col("Payment_type") == 2, "Cash")
    .when(col("Payment_type") == 3, "No charge")
    .when(col("Payment_type") == 4, "Dispute")
    .when(col("Payment_type") == 5, "Unknown")
    .when(col("Payment_type") == 6, "Voided trip")
    .otherwise(col("Payment_type"))
)


# Change data type of columns. The default datatype inferred by spark can be inaccurate.
df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(StringType()))
df = df.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(StringType()))
df = df.withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
df = df.withColumn("trip_distance", col("trip_distance").cast(FloatType()))
df = df.withColumn("fare_amount", col("fare_amount").cast(FloatType()))
df = df.withColumn("extra", col("extra").cast(FloatType()))
df = df.withColumn("mta_tax", col("mta_tax").cast(FloatType()))
df = df.withColumn("tip_amount", col("tip_amount").cast(FloatType()))
df = df.withColumn("tolls_amount", col("tolls_amount").cast(FloatType()))
df = df.withColumn("improvement_surcharge", col("improvement_surcharge").cast(FloatType()))
df = df.withColumn("total_amount", col("total_amount").cast(FloatType()))
df = df.withColumn("congestion_surcharge", col("congestion_surcharge").cast(FloatType()))
df = df.withColumn("Airport_fee", col("Airport_fee").cast(FloatType()))


# Rename columns
df = df.withColumnRenamed('VendorID', 'vendor_name')
df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')
df = df.withColumnRenamed('tpep_pickup_datetime_day', 'pickup_datetime_day')
df = df.withColumnRenamed('tpep_pickup_datetime_month', 'pickup_datetime_month')
df = df.withColumnRenamed('tpep_pickup_datetime_year', 'pickup_datetime_year')
df = df.withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
df = df.withColumnRenamed('tpep_dropoff_datetime_day', 'dropoff_datetime_day')
df = df.withColumnRenamed('tpep_dropoff_datetime_month', 'dropoff_datetime_month')
df = df.withColumnRenamed('tpep_dropoff_datetime_year', 'dropoff_datetime_year')


# Change order of columns
new_column_order = ["vendor_name",
"pickup_datetime",
"pickup_datetime_day",
"pickup_datetime_month",
"pickup_datetime_year",
"dropoff_datetime",
"dropoff_datetime_day",
"dropoff_datetime_month",
"dropoff_datetime_year",
"passenger_count",
"trip_distance",
"RatecodeID",
"store_and_fwd_flag",
"PULocationID",
"DOLocationID",
"payment_type",
"fare_amount",
"extra",
"mta_tax",
"tip_amount",
"tolls_amount",
"improvement_surcharge",
"total_amount",
"congestion_surcharge",
"airport_fee"]
df = df.select(*new_column_order)


# Load data into BigQuery table

# Use the Cloud Storage bucket for temporary BigQuery export data usedby the connector.
bucket = "data_lake_<project_id>"
spark.conf.set('temporaryGcsBucket', bucket)

df.write.format('bigquery') \
  .option('table', 'nyctaxi.yellow_tripdata11') \
  .mode('append') \
  .save()
