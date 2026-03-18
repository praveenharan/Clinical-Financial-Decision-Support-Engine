# 1. Remove records without a Transaction_ID or Department_ID (Critical for Finance)
# 2. Fill null Denial_Codes with 'CLEAN' for easier analysis
# 3. Standardize Charge_Amount to absolute values
df_cleansed = df_with_metadata.filter(col("Transaction_ID").isNotNull()) \
    .filter(col("Department_ID").isNotNull()) \
    .withColumn("Denial_Code", when(col("Denial_Code").isNull(), lit("NONE")).otherwise(col("Denial_Code"))) \
    .withColumn("Charge_Amount", when(col("Charge_Amount") < 0, col("Charge_Amount") * -1).otherwise(col("Charge_Amount")))

# 4. Deduplication logic based on Transaction_ID
df_silver = df_cleansed.dropDuplicates(["Transaction_ID"])

silver_table_path = "Tables/Silver_Clinical_Billing"

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver_Clinical_Billing")

print(f"Successfully processed {df_silver.count()} records into the Silver Layer.")
