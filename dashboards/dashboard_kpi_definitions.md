# Dashboard KPI Definitions

## Executive Overview Dashboard

### Total Orders
Total number of customer orders processed for a given business date.

---

### Net Revenue
Total revenue generated from completed customer orders.

---

### Average Order Value (AOV)
Average revenue generated per order.

Formula:

```sql
net_revenue / total_orders
```

---

### Completed Orders
Total successfully fulfilled customer orders.

---

### Failed Orders
Total customer orders that failed due to operational or delivery-related reasons.

---

### Rolling 7-Day Average Orders
7-day moving average of total daily orders used to smooth short-term fluctuations and identify trends.

---

### Rolling 7-Day Average Revenue
7-day moving average of daily revenue performance.

---

### Rolling 7-Day Average AOV
7-day moving average of Average Order Value.

---

### Rolling 7-Day Completed Orders
7-day moving average of successfully completed orders.

---

### Rolling 7-Day Failed Orders
7-day moving average of failed orders used for operational monitoring.

---

# Customer Activity Dashboard

### Customer Count Trend
Tracks daily customer activity and customer participation trends over time.

---

# Restaurant Analytics Dashboard

### Top Restaurants by Revenue
Ranks restaurants based on total revenue contribution.

---

### Top Restaurants by Orders
Ranks restaurants based on total order volume processed.

---

### Revenue Distribution
Shows revenue contribution distribution among the highest-performing restaurants.

---

### Completion Rate
Measures operational fulfillment success at the restaurant level.

Formula:

```sql
(completed_orders * 100) / total_orders
```

---

# Pipeline Monitoring Dashboard

### Silver Layer Health
Displays overall Silver layer batch execution status distribution.

Possible statuses:
- SUCCESS
- PARTIAL_SUCCESS
- FAILED
- QUARANTINED

---

### Silver Data Quality Trend
Tracks invalid record percentages across Silver layer processing batches.

---

### Silver Data Change Behaviour
Tracks incremental processing behavior including:
- Inserted Rows
- Updated Rows
- Stale Rows
- Late Arriving Rows

---

### Silver Layer Performance
Measures average Silver layer processing throughput using rows processed per second.

---

### Bronze Data Change Behaviour
Tracks ingestion-level operational metrics across Bronze processing batches.

---

### Bronze Layer Health
Displays Bronze layer ingestion success/failure distribution.

Possible statuses:
- SUCCESS
- FAILED
- SKIPPED
- EMPTY