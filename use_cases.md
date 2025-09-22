# Use Cases for Enterprise Data Quality Framework

## 1. Basic Validation with Single Rule Pass/Fail

- Table: `sales_data`
- Technical rule: `ExpectColumnValuesToNotBeNull` on `customer_id`
- Pass scenario: All `customer_id` values present
- Fail scenario: Some `customer_id` NULLs

## 2. Multiple Rules with Mixed Results and Scoring

- Business rules include unique constraints, value sets, and thresholds
- Technical rules validate column types, formats
- Weight and criticality influence final DQ score
- Example: 3 business rules, 2 technical rules; 2 rules fail → weighted score drops below threshold → DQ fail

## 3. Partition Filter Enforcement

- Validation must specify at least one partition column in filter
- If missing, framework blocks validation unless `force_run=True`
- Example: filter on date partition `date_col='2025-01-01'`

## 4. Audit Report Analysis

- Audit records contain detailed rule results JSON
- Users can query by partition, execution date, batch ID
- Viewing recent passes/fails to prioritize remediation

## 5. Embedding in ETL Pipeline

- ETL job invokes validation with current Spark session and partition filter
- Upon failure, ETL throws exception or reports error for alerting

## 6. Handling Different Criticalities

- Rules marked `critical` cause larger impact on DQ score
- Multiple critical rule failures may halt pipeline

## 7. Testing with Mock Data

- Tests cover rules triggering success/failure
- Covers all corner cases: empty data, missing partitions, mixed results

---

### Mocked Data Example: sales_data

| customer_id | order_id | amount | order_date  |
|-------------|----------|--------|-------------|
| 1001        | 5001     | 100.0  | 2025-01-01  |
| NULL        | 5002     | 200.0  | 2025-01-01  |
| 1003        | NULL     | NULL   | 2025-01-02  |

---

This use case document helps developers and users understand expected inputs, outputs, failures, and scoring behavior in real-world enterprise validations.
