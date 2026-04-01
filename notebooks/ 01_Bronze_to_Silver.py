from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Define the expected schema for Clinical Billing
bronze_schema = StructType([
    StructField("Transaction_ID", StringType(), False),
    StructField("Patient_ID", StringType(), True),
    StructField("Provider_ID", StringType(), True),
    StructField("Department_ID", StringType(), True),
    StructField("Service_Date", TimestampType(), True),
    StructField("Charge_Amount", DoubleType(), True),
    StructField("Contractual_Allowance", DoubleType(), True),
    StructField("Denial_Code", StringType(), True),
    StructField("Payer_ID", StringType(), True)
])


# Path to your ADLS Gen2 / Fabric Lakehouse Bronze folder
bronze_path = "Files/Bronze/Billing_Raw/*.parquet"

df_raw = spark.\
    read.\
    format("parquet").\
    schema(bronze_schema).\
    load(bronze_path)

# Add Audit Metadata
df_with_metadata = df_raw.\
    withColumn("Ingestion_Timestamp", current_timestamp()).\
    withColumn("Source_File", input_file_name())

# 1. Remove records without a Transaction_ID or Department_ID (Critical for Finance)
# 2. Fill null Denial_Codes with 'CLEAN' for easier analysis
# 3. Standardize Charge_Amount to absolute values
df_cleansed = df_with_metadata.\
    filter(col("Transaction_ID").isNotNull()).\
    filter(col("Department_ID").isNotNull()).\
    withColumn("Denial_Code", when(col("Denial_Code").isNull(), lit("NONE")).otherwise(col("Denial_Code"))).\
    withColumn("Charge_Amount", when(col("Charge_Amount") < 0, col("Charge_Amount") * -1).otherwise(col("Charge_Amount")))

# 4. Deduplication logic based on Transaction_ID
df_silver = df_cleansed.\
    dropDuplicates(["Transaction_ID"])

silver_table_path = "Tables/Silver_Clinical_Billing"

df_silver.write.\
    format("delta").\
    mode("overwrite").\
    option("overwriteSchema", "true").\
    saveAsTable("Silver_Clinical_Billing")

print(f"Successfully processed {df_silver.count()} records into the Silver Layer.")

