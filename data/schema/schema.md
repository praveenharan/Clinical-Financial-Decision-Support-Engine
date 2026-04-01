# Schema
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
