# Healthcare Data Engineering: Synthetic Patient Ingestion

This repository contains a PySpark implementation for generating synthetic healthcare data and ingesting it into a **Lakehouse Bronze Layer**. The process utilizes the `Faker` library to create realistic patient demographics and persists the data using the **Delta** format.

## Overview

The script performs the following steps:
1. **Environment Setup**: Installs necessary libraries.
2. **Schema Definition**: Establishes a strict `StructType` for patient records.
3. **Data Generation**: Uses `Faker` and `random` to create 100 unique patient records.
4. **Data Persistence**: Writes the resulting DataFrame to a Delta table named `Dim_Patients`.

## Implementation
# 1. Initialize Faker
```python
fake = Faker()
```
# 2. Define Schema
# Defining a strict schema ensures data quality during the ingestion phase.
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
