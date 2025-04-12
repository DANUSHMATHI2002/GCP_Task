---

# Python & SQL Assignment Project

This project demonstrates solutions to a data engineering and analytics assignment using **Python** and **SQL**. It involves simulating an ETL pipeline, performing data deduplication, running analytical SQL queries, and identifying VIP customers based on purchase data.

---

## 📁 Project Structure

```
python_sql_assignment_project/
├── product_reviews.csv               # Raw product review data
├── purchases.csv                     # Customer purchase data
├── members.csv                       # Membership data
├── users.csv                         # User data
├── cleaned_reviews.csv               # Cleaned product reviews (generated)
├── deduplicated_users.csv            # Deduplicated user data (generated)
├── vip_customers.csv                 # VIP customer data (generated)
├── etl_pipeline.py                   # ETL pipeline script
├── deduplicate_users.py              # User deduplication script
├── identify_vip_customers.py         # VIP customer identification script
```

---

## ✅ Tasks Covered

### 1. ETL Pipeline Simulation (Python)

This task involves simulating an ETL pipeline for product reviews.

- **Extract**: Loads the `product_reviews.csv` file.
- **Transform**:
  - Drops rows with missing `rating` or `review_text`.
  - Converts `rating` to an integer and filters invalid values.
  - Generates a `sentiment` column based on a simple rule:
    - If the review text contains the word "bad" → Negative.
    - Else → Positive.
- **Load**: Saves the cleaned reviews to `cleaned_reviews.csv`.

> Run the ETL pipeline script:

```bash
python etl_pipeline.py
```

### 2. Data Deduplication (Python)

This task focuses on deduplicating user data by removing duplicate emails and keeping only the latest entry based on the `last_updated` field.

- **Input**: `users.csv`
- **Output**: `deduplicated_users.csv`

> Run the deduplication script:

```bash
python deduplicate_users.py
```

### 3. SQL Query: Users with >3 Orders in Last 90 Days

This SQL query identifies users who have placed more than 3 orders in the last 90 days.

- **Join**: Users table with the orders table.
- **Filter**: Orders placed within the last 90 days.
- **Group**: By user ID and name, counting orders and summing the total amount spent.
- **Return**: Users with more than 3 orders.

```sql
SELECT u.user_id, u.name, COUNT(o.order_id) AS order_count, SUM(o.amount) AS total_spent
FROM users u
JOIN orders o ON u.user_id = o.user_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY u.user_id, u.name
HAVING COUNT(o.order_id) > 3;
```

### 4. SQL Query: Missing Hourly Sensor Logs in 24 Hours

This SQL query generates hourly timestamps for the past 24 hours and identifies missing hourly sensor logs.

- **Generate**: Hourly timestamps for the last 24 hours.
- **Join**: With all sensors to create an expected log.
- **Compare**: Actual logs against the expected ones.
- **Return**: Missing hours per sensor.

```sql
WITH hours AS (
  SELECT generate_series(
    date_trunc('hour', now() - INTERVAL '24 hours'),
    date_trunc('hour', now()),
    INTERVAL '1 hour'
  ) AS hour
),
sensors AS (
  SELECT DISTINCT sensor_id FROM sensor_logs
),
expected AS (
  SELECT s.sensor_id, h.hour FROM sensors s CROSS JOIN hours h
),
actual AS (
  SELECT sensor_id, date_trunc('hour', timestamp) AS hour
  FROM sensor_logs WHERE timestamp >= now() - INTERVAL '24 hours'
)
SELECT e.sensor_id, e.hour AS missing_hour
FROM expected e
LEFT JOIN actual a ON e.sensor_id = a.sensor_id AND e.hour = a.hour
WHERE a.hour IS NULL;
```

### 5. Identify VIP Customers (Python + SQL)

This task identifies VIP customers based on their total purchase amount and membership level.

- **Input**: `purchases.csv` and `members.csv`
- **Output**: `vip_customers.csv`
  
  **Python Version**:
  - Groups purchases by `customer_id` and calculates the total purchase amount.
  - Filters customers who have spent more than $250 and have a "Gold" membership level.

  > Run the VIP customer identification script:

  ```bash
  python identify_vip_customers.py
  ```

  **SQL Version**:

  ```sql
  SELECT m.customer_id, SUM(p.amount) AS total_spent, m.membership_level
  FROM purchases p
  INNER JOIN members m ON p.customer_id = m.customer_id
  WHERE m.membership_level = 'Gold'
  GROUP BY m.customer_id, m.membership_level
  HAVING SUM(p.amount) > 250;
  ```

---

## 🛠 Tech Used

- Python 3.8+
- **pandas** for data manipulation
- **SQL** (PostgreSQL syntax) for database queries
- **CSV file I/O** for data storage and exchange

---




