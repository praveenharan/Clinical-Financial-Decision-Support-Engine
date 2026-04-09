# Schema

### dim_patients
```python
root
 |-- PatientKey: integer (nullable = false)
 |-- MRN: string (nullable = false)
 |-- Name: string (nullable = true)
 |-- DOB: date (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Insurance_Provider: string (nullable = true)
```

### dim_physicians
```python
root
 |-- PhysicianKey: integer (nullable = false)
 |-- NPI_Number: string (nullable = false)
 |-- Name: string (nullable = true)
 |-- Specialty: string (nullable = true)
 |-- Department: string (nullable = true)
```

### dim_date
```python
root
 |-- DateKey: integer (nullable = true)
 |-- Date: date (nullable = false)
 |-- Year: integer (nullable = false)
 |-- Quarter: integer (nullable = false)
 |-- Month: integer (nullable = false)
 |-- MonthName: string (nullable = false)
 |-- Day: integer (nullable = false)
 |-- DayOfWeek: integer (nullable = false)
 |-- DayName: string (nullable = false)
 |-- DayType: string (nullable = false)
```

### dim_diagnosis
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
 |-- PhysicianKey: long (nullable = true)
 |-- AdmissionKey: long (nullable = true)
 |-- Flag_Abnormal: string (nullable = false)

```
