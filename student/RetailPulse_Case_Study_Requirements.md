# RetailPulse Weekly Sales Intelligence and Event Notification

## Case-study paragraph

RetailPulse is a growing omnichannel retail company that receives customer,
product and sales-order data from different operational systems. The company
wants fresher data engineers to build a small lakehouse pipeline in Databricks
Free Edition. The trainees must load three CSV datasets, clean and transform
them with PySpark, reproduce important analysis with Spark SQL, store the
curated results as managed Delta tables, create a compact sales-summary event,
publish that event to an Apache Kafka topic, consume it with a Kafka consumer,
and schedule the complete workflow through Apache Airflow every Wednesday.
The implementation should remain simple, readable and suitable for classroom
demonstration.

## Datasets

| Dataset | Rows | Purpose |
|---|---:|---|
| `customers_500.csv` | 500 | Customer profile, segment, location, activity and update history |
| `products_500.csv` | 500 | Product, category, price, cost, supplier, stock and active flag |
| `sales_orders_500.csv` | 500 | Orders, quantities, discounts, status and delivery information |

The datasets intentionally contain duplicate customer versions, missing values,
invalid prices, inactive records, zero/negative quantities, pending orders,
cancelled orders and returned orders.

## Business requirements

### Part A — Databricks setup and Bronze layer

1. Create a schema named `retail_fresher`.
2. Create a managed volume named `retail_raw`.
3. Upload the three CSV files into the volume.
4. Read every CSV with PySpark.
5. Keep the initial CSV columns as strings in the Bronze layer.
6. Add `source_file` and `ingestion_timestamp`.
7. Save managed Delta tables:
   - `bronze_customers`
   - `bronze_products`
   - `bronze_sales_orders`

### Part B — PySpark transformations

1. Display schema and sample rows.
2. Trim string columns and standardize upper/lower case where appropriate.
3. Replace missing city values with `Unknown`.
4. Cast dates, timestamps, integers and decimal columns.
5. Deduplicate customer profiles using `row_number()` and the latest
   `updated_at`.
6. Filter inactive customers.
7. Remove products with invalid prices or costs.
8. Filter inactive products.
9. Filter orders with quantity less than or equal to zero.
10. Exclude `PENDING` and `CANCELLED` orders from financial analysis.
11. Add:
    - `gross_amount`
    - `discount_amount`
    - `net_amount`
    - `net_sales`
    - `profit_per_unit`
    - `delivery_days`
    - `late_delivery_flag`
    - `order_month`
12. Join customers, products and orders.
13. Store Silver managed Delta tables.
14. Create Gold tables:
    - monthly category sales
    - city sales
    - customer value
    - top products by category

### Part C — Aggregation and window functions

Implement:

1. `GROUP BY` category and month.
2. `SUM`, `COUNT`, `AVG`, `MIN` and `MAX`.
3. `row_number()` to keep the latest customer profile.
4. `rank()` to identify top products inside each category.
5. `dense_rank()` to rank customers within each state.
6. A running revenue total by category and month.
7. The latest order for every customer.

### Part D — Spark SQL

Using the managed Delta tables:

1. Filter completed orders.
2. Join the three datasets.
3. Use `CAST`, `CASE WHEN`, date functions and string functions.
4. Create a monthly sales summary.
5. Rank products by category revenue.
6. Find the top three customers in each state.
7. Compare the Spark SQL results with the PySpark results.

### Part E — Kafka event

Create one event per month and category with this structure:

```json
{
  "event_id": "SALES-2026-01-Electronics",
  "event_type": "MONTHLY_CATEGORY_SALES_READY",
  "sales_month": "2026-01",
  "category": "Electronics",
  "order_count": 10,
  "total_quantity": 28,
  "total_revenue": 250000.00
}
```

Publish the events to:

```text
Topic: retail-sales-summary
```

Create a consumer that prints every message and stores it in
`consumed_events.jsonl`.

### Part F — Airflow automation

Create an Airflow DAG named:

```text
retailpulse_weekly_orchestration
```

Required flow:

```text
Trigger Databricks Job
        ↓
Wait for Databricks completion
        ↓
Read Gold event rows
        ↓
Publish events to Kafka
        ↓
Consume a sample of events
        ↓
Finish
```

Schedule it every Wednesday at 09:00:

```text
0 9 * * 3
```

Use `catchup=False`.

## Expected outcome

At the end, trainees should have:

- Three Bronze managed Delta tables.
- Clean Silver customer, product, order and enriched-sales tables.
- Four Gold analytical tables.
- PySpark transformations demonstrating filters, casts, joins, derived columns,
  aggregations and windows.
- Equivalent Spark SQL queries.
- JSON sales-summary events.
- Kafka producer and consumer output.
- An Airflow DAG that triggers the Databricks job every Wednesday.
- A short validation report comparing input, rejected and processed rows.

## Submission checklist

- [ ] Databricks notebooks exported as source files
- [ ] Screenshot of Bronze/Silver/Gold tables
- [ ] PySpark output screenshots
- [ ] Spark SQL output screenshots
- [ ] Kafka producer log
- [ ] Kafka consumer log
- [ ] Airflow Graph view
- [ ] Airflow successful run
- [ ] Data-quality/control-total document
