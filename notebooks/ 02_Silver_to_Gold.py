
from pyspark.sql.functions import col, sum, avg, count, date_format, year, month

# Load cleaned Silver data
df_silver = spark.\
    read.\
    table("Silver_Clinical_Billing")

# Extract unique departments for the Dimension Table
df_dim_dept = df_silver.\
    select("Department_ID").\
    distinct().\
    withColumn("Department_Name", col("Department_ID")) # In a real scenario, join with a Master Dept list

df_dim_dept.\
    write.\
    format("delta").\
    mode("overwrite").\
    saveAsTable("Dim_Department")

# Adding Time-based columns for easier Power BI Time Intelligence (YTD, QTD)
df_fact_billing = df_silver.\
    withColumn("Fiscal_Year", year(col("Service_Date"))).\
    withColumn("Fiscal_Month", month(col("Service_Date"))).\
    withColumn("Month_Name", date_format(col("Service_Date"), "MMMM"))

# Select final columns for the Gold Fact Table
gold_columns = [
    "Transaction_ID", "Provider_ID", "Department_ID", "Payer_ID", 
    "Fiscal_Year", "Fiscal_Month", "Month_Name", "Service_Date",
    "Charge_Amount", "Contractual_Allowance", "Denial_Code"
]

df_fact_billing = df_fact_billing.\
    select(*gold_columns)

# Write to Gold Table
df_fact_billing.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Fact_Clinical_Billing")

# 1. Z-Order Optimization: 
# We Z-Order by Department_ID and Payer_ID because these are the 
# most common 'Filters' used in Financial Drill-Down reports.
spark.sql("OPTIMIZE Fact_Clinical_Billing ZORDER BY (Department_ID, Payer_ID)")

# 2. Vacuum: 
# Clean up old file versions to save storage costs (Retention set to 7 days by default)
spark.sql("VACUUM Fact_Clinical_Billing RETAIN 168 HOURS")

print("Gold Layer Fact Table created and optimized with Z-Ordering.")
