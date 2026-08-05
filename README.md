# RetailPulse PySpark, Spark SQL, Kafka and Airflow Case Study

## Download contents

### Student material

- `student/RetailPulse_Case_Study_Requirements.md`
- `student/Practical_Questions.md`
- Three CSV datasets with 500 rows each
- Excel workbook with all datasets

### Answer material

- Five Databricks source notebooks
- Kafka producer and consumer
- Airflow orchestration DAG
- Detailed answer key
- Expected output and control totals

## Case-study flow

```text
CSV datasets
→ Databricks Bronze
→ PySpark Silver
→ PySpark/Spark SQL Gold
→ Kafka event
→ Kafka consumer
→ Airflow weekly schedule
```

## Dataset volume

```text
Customers     : 500 rows
Products      : 500 rows
Sales orders  : 500 rows
Total         : 1,500 input rows
```

## Start

Read:

```text
docs/Implementation_Guide.md
```

Then provide only the `student` folder and `datasets` folder to trainees.

Keep the `answers` folder separately for trainer evaluation.
