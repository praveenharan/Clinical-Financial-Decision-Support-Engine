# Schema



### 
```python

```

### 
```python

```

### 
```python

```

### 
```python

```

### df_diagnosis
```python
root
 |-- DiagnosisKey: integer (nullable = false)
 |-- ICD10_Code: string (nullable = false)
 |-- Description: string (nullable = true)
 |-- Category: string (nullable = true)
```

### fact_admissions
```python
root
 |-- AdmissionKey: long (nullable = false)
 |-- PatientKey: integer (nullable = true)
 |-- PhysicianKey: integer (nullable = true)
 |-- DiagnosisKey: integer (nullable = true)
 |-- AdmissionDateKey: integer (nullable = true)
 |-- Length_of_Stay: integer (nullable = true)
 |-- DischargeDateKey: integer (nullable = true)
 |-- Total_Cost: double (nullable = true)
```

### fact_lab_results
```python
fact_lab_results.printSchema()

root
 |-- LabKey: long (nullable = false)
 |-- PatientKey: integer (nullable = true)
 |-- Test_DateKey: integer (nullable = true)
 |-- Test_Type: string (nullable = false)
 |-- Result_Value: double (nullable = true)
 |-- Flag_Abnormal: string (nullable = false)

```
