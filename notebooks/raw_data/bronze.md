# Healthcare Data Engineering: Synthetic Patient Ingestion

This repository contains a PySpark implementation for generating synthetic healthcare data and ingesting it into a **Lakehouse Bronze Layer**. The process utilizes the `Faker` library to create realistic patient demographics and persists the data using the **Delta** format.

## Overview

The script performs the following steps:
1. **Environment Setup**: Installs necessary libraries.
2. **Schema Definition**: Establishes a strict `StructType` for patient records.
3. **Data Generation**: Uses `Faker` and `random` to create 100 unique patient records.
4. **Data Persistence**: Writes the resulting DataFrame to a Delta table named `Dim_Patients`.

## Implementation
### Patients Data

```python
# Install Faker library
%pip install faker

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, expr, date_format
import random
from faker import Faker

# 1. Initialize Faker
fake = Faker()

# 2. Define Schema
# Defining a strict schema ensures data quality during the ingestion phase.
schema = StructType([
    StructField("PatientKey", IntegerType(), False),
    StructField("MRN", StringType(), False),
    StructField("Name", StringType(), True),
    StructField("DOB", DateType(), True),
    StructField("Gender", StringType(), True),
    StructField("Insurance_Provider", StringType(), True)
])

# 3. Synthetic Data Generation
providers = ["Blue Cross Blue Shield", "Aetna", "UnitedHealthcare", "Cigna", "Kaiser Permanente", "Medicare"]
genders = ["M", "F", "Other", "U"]

data = []
for i in range(1, 101):
    data.append((
        i, 
        f"MRN-{random.randint(100000, 999999)}", 
        fake.name(), 
        fake.date_of_birth(minimum_age=0, maximum_age=90), 
        random.choice(genders), 
        random.choice(providers)
    ))

# 4. Create DataFrame
df_patients = spark.createDataFrame(data, schema=schema)

# 5. Write to Lakehouse (Bronze Layer)
# Using Delta format for ACID compliance and schema enforcement
df_patients.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Dim_Patients")
