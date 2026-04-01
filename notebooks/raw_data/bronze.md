# Healthcare Data Engineering: Synthetic Patient Ingestion

This repository contains a PySpark implementation for generating synthetic healthcare data and ingesting it into a **Lakehouse Bronze Layer**. The process utilizes the `Faker` library to create realistic patient demographics and persists the data using the **Delta** format.

## Overview

The script performs the following steps:
1. **Environment Setup**: Installs necessary libraries.
2. **Schema Definition**: Establishes a strict `StructType` for patient records.
3. **Data Generation**: Uses `Faker` and `random` to create 100 unique patient records.
4. **Data Persistence**: Writes the resulting DataFrame to a Delta table named `Dim_Patients`.

## Implementation
# 1. Environment Setup & Library Imports
```python
%pip install faker

fake = Faker()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, expr, date_format, year, month, dayofmonth, quarter, dayofweek, explode, floor, rand, round, when
import random
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from datetime import date, timedelta

```
# 2. Define Schema
# Defining a strict schema ensures data quality during the ingestion phase.
### Patient
```python
patient_schema = StructType([
    StructField("PatientKey", IntegerType(), False),
    StructField("MRN", StringType(), False),
    StructField("Name", StringType(), True),
    StructField("DOB", DateType(), True),
    StructField("Gender", StringType(), True),
    StructField("Insurance_Provider", StringType(), True)
])
```
### Physician
```python

physician_schema = StructType([
    StructField("PhysicianKey", IntegerType(), False),
    StructField("NPI_Number", StringType(), False),
    StructField("Name", StringType(), True),
    StructField("Specialty", StringType(), True),
    StructField("Department", StringType(), True)
])
```
### Date
```python
# 1. Define date range
start_date = "2020-01-01"
end_date = (date.today() - timedelta(days=1)).isoformat()

# 2. Generate the sequence and explode into a single column named "FullDate"
df_base = spark.range(1).select(
    expr(f"sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)").alias("date_array")
)

df_exploded = df_base.select(explode(col("date_array")).alias("FullDate"))

# 3. Derive attributes using the "FullDate" reference
dim_date = df_exploded.select(
    date_format(col("FullDate"), "yyyyMMdd").cast("int").alias("DateKey"),
    col("FullDate").alias("Date"),
    year(col("FullDate")).alias("Year"),
    quarter(col("FullDate")).alias("Quarter"),
    month(col("FullDate")).alias("Month"),
    date_format(col("FullDate"), "MMMM").alias("MonthName"),
    dayofmonth(col("FullDate")).alias("Day"),
    dayofweek(col("FullDate")).alias("DayOfWeek"),
    date_format(col("FullDate"), "EEEE").alias("DayName"),
    expr("CASE WHEN dayofweek(FullDate) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END").alias("DayType")
)

# 4. Write to Lakehouse
dim_date.write.format("delta").mode("overwrite").saveAsTable("Dim_Date")

display(dim_date.limit(10))
```
### Physician
```python

```
### Physician
```python

```
### Physician
```python

```
### Physician
```python

# 3. Synthetic Data Generation
```python
providers = ["Blue Cross Blue Shield", "Aetna", "UnitedHealthcare", "Cigna", "Kaiser Permanente", "Medicare"]
genders = ["M", "F", "Other", "U"]

patient_data = []
for i in range(1, 101):
    patient_data.append((
        i, 
        f"MRN-{random.randint(100000, 999999)}", 
        fake.name(), 
        fake.date_of_birth(minimum_age=0, maximum_age=90), 
        random.choice(genders), 
        random.choice(providers)
    ))
```
# 4. Create DataFrame
```python
df_patients = spark.createDataFrame(patient_data, schema=patient_schema)
```

# 5. Write to Lakehouse (Bronze Layer)
# Using Delta format for ACID compliance and schema enforcement

```python
df_patients.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Dim_Patients")

display(df_patients.limit(10))
```

# 1. Define Schema
# Defining a strict schema ensures data quality during the ingestion phase.
```python
physician_schema = StructType([
    StructField("PhysicianKey", IntegerType(), False),
    StructField("NPI_Number", StringType(), False),
    StructField("Name", StringType(), True),
    StructField("Specialty", StringType(), True),
    StructField("Department", StringType(), True)
])
```
# 2. Define lists for realistic data
specialties = [
    ("Cardiology", "Internal Medicine"),
    ("Pediatrics", "Primary Care"),
    ("Neurology", "Specialized Medicine"),
    ("Orthopedics", "Surgery"),
    ("Dermatology", "Ambulatory"),
    ("Oncology", "Internal Medicine"),
    ("Emergency Medicine", "Acute Care"),
    ("Radiology", "Diagnostics")
]

# 3. Synthetic Data Generation
# Generate 10 Physician records
physician_data = []
for i in range(1, 11):
    spec_dept = random.choice(specialties)
    physician_data.append((
        i,                                           # PhysicianKey
        str(random.randint(1000000000, 1999999999)), # NPI (10-digit)
        f"Dr. {fake.name()}",                        # Name
        spec_dept[0],                                # Specialty
        spec_dept[1]                                 # Department
    ))

# 4. Create DataFrame
df_physicians = spark.createDataFrame(physician_data, schema=physician_schema)

# 5. Write to Lakehouse (Bronze Layer)
# Using Delta format for ACID compliance and schema enforcement
df_physicians.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Dim_Physician")

display(df_physicians.limit(10))
