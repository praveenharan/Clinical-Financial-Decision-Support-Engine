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