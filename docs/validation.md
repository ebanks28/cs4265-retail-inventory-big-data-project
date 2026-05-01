# Validation Report
 
## Scalable Retail Inventory Analytics — Pipeline Data Quality & Testing
 
This document reports the data quality metrics, validation evidence, edge case behavior,
and performance characteristics of the multi-source analytics pipeline
(`src/multi_source_retail_analytics.scala`).
 
---
 
## 1. Record Counts at Each Pipeline Stage
 
The following counts were recorded during a full end-to-end pipeline run on the clean
dataset without synthetic scaling.
 
| Stage | Record Count | Notes |
|-------|-------------|-------|
| Raw UCI retail ingestion | 541,909 | All rows from `retail.csv` before any filtering |
| After cleaning (retail) | 397,884 | Nulls, cancellations, and invalid rows removed |
| Rows removed by cleaning | 144,025 | 26.5% of raw rows filtered out |
| GDP records loaded | 261 | Country-level GDP entries from World Bank dataset |
| Rows matched to GDP | 396,832 | 99.7% of clean retail rows enriched with GDP data |
| Rows unmatched to GDP | 1,052 | 3 unmappable country values (see Section 4) |
| Countries in retail data | 37 | Distinct country values in clean retail dataset |
| Countries matched to GDP | 34 | Countries successfully joined to World Bank data |
| Countries unmatched | 3 | Channel Islands, European Community, Unspecified |
 
---
 
## 2. Cleaning Transformation Validation
 
The following filters are applied during cleaning. Each is validated below.
 
### 2.1 Null CustomerID Removal
 
**Rule:** Rows where `CustomerID IS NULL` are dropped.
 
**Validation:** The raw dataset contains rows with no customer identifier, which cannot
be used in customer-level analysis. After cleaning, a `filter(col("CustomerID").isNotNull)`
check confirms zero null CustomerIDs remain in the clean dataset.
 
### 2.2 Cancellation Removal
 
**Rule:** Rows where `InvoiceNo` starts with `C` are dropped.
 
**Validation:** Cancellation records use negative quantities to reverse prior transactions.
Including them without matching reversal logic would undercount revenue. After filtering,
no InvoiceNo values starting with `C` remain in the clean dataset.
 
### 2.3 Invalid Quantity and Price Removal
 
**Rule:** Rows where `Quantity <= 0` or `UnitPrice <= 0` are dropped.
 
**Validation:** Non-positive quantities and prices represent data entry errors or
administrative records not corresponding to real sales. Their removal does not affect
analytical validity.
 
### 2.4 Revenue Derivation
 
**Rule:** `Revenue = Quantity * UnitPrice`
 
**Spot check — United Kingdom (from Query 1 output):**
- `TotalRevenue_GBP`: £7,308,391.55
- `NumInvoices`: 16,646
- `NumCustomers`: 3,920
- `RevenuePerCustomer`: £1,864.39
**Manual verification:**
£7,308,391.55 / 3,920 customers = £1,864.38 per customer ✓
(£0.01 difference due to rounding in the query)
 
**Spot check — Netherlands:**
- `TotalRevenue_GBP`: £285,446.34
- `NumCustomers`: 9
- `RevenuePerCustomer`: £31,716.26
**Manual verification:**
£285,446.34 / 9 customers = £31,716.26 per customer ✓
 
### 2.5 Date Parsing Validation
 
**Rule:** `InvoiceDate` is parsed using `coalesce(to_timestamp(..., "yyyy/MM/dd HH:mm:ss"), to_timestamp(..., "MM/dd/yyyy HH:mm"))`
 
**Validation:** Prior to adding the `coalesce` fallback, an incorrect format string caused
all `InvoiceDateParsed` values to return null, producing an empty result for the monthly
trend query (Query 4). After the fix, Query 4 produces non-empty results across all
months in the dataset, confirming successful date parsing.
 
---
 
## 3. Multi-Source Join Validation
 
### 3.1 Join Match Rate
 
| Metric | Value |
|--------|-------|
| Total clean retail rows | 397,884 |
| Rows matched to GDP | 396,832 |
| Match rate | 99.7% |
| Countries matched | 34 / 37 |
 
### 3.2 Country Name Normalization Spot Checks
 
The following mappings were validated by confirming that joined rows contain non-null
`GDP_2011` values for each country:
 
| UCI Country | Mapped To | GDP_Billions_USD | Match Status |
|-------------|-----------|-----------------|--------------|
| United Kingdom | United Kingdom | 2,675.59 | ✓ Matched |
| EIRE | Ireland | 240.98 | ✓ Matched |
| USA | United States | 15,599.73 | ✓ Matched |
| Czech Republic | Czechia | 231.43 | ✓ Matched |
| Netherlands | Netherlands | 913.14 | ✓ Matched |
| Germany | Germany | 3,823.58 | ✓ Matched |
 
### 3.3 Unmatched Countries
 
The following 3 country values in the UCI dataset have no valid World Bank equivalent
and are intentionally excluded from GDP-dependent queries:
 
| UCI Country | Reason |
|-------------|--------|
| Channel Islands | British Crown dependency; not a standalone World Bank entry |
| European Community | Supranational entity; no country-level GDP |
| Unspecified | No country information available |
 
These 3 values account for 1,052 unmatched rows (0.26% of clean retail data),
which is not material to analytical results.
 
---
 
## 4. Query Output Validation
 
### 4.1 Query 1 — Revenue by Country with GDP Context (Top 5)
 
| Country | GDP_Billions_USD | TotalRevenue_GBP | NumInvoices | NumCustomers | RevenuePerCustomer |
|---------|-----------------|-----------------|-------------|-------------|-------------------|
| United Kingdom | 2,675.59 | 7,308,391.55 | 16,646 | 3,920 | 1,864.39 |
| Netherlands | 913.14 | 285,446.34 | 94 | 9 | 31,716.26 |
| EIRE | 240.98 | 265,545.90 | 260 | 3 | 88,515.30 |
| Germany | 3,823.58 | 228,867.14 | 457 | 94 | 2,434.76 |
| France | 2,870.41 | 209,024.05 | 389 | 87 | 2,402.58 |
 
**Reasonableness check:** The United Kingdom dominates revenue as expected — the UCI
dataset is sourced from a UK-based retailer. The high RevenuePerCustomer for EIRE
(£88,515.30) reflects only 3 customers placing 260 invoices, suggesting a small number
of high-volume wholesale buyers, which is consistent with the dataset's B2B nature.
 
### 4.2 Query 5 — Source Integration Diagnostics
 
| Metric | Value |
|--------|-------|
| TotalRetailRows | 397,884 |
| UniqueCountriesInRetail | 37 |
| RowsMatchedToGDP | 396,832 |
| CountriesMatchedToGDP | 34 |
| CountriesUnmatched | 3 |
 
---
 
## 5. Edge Case Behavior
 
### 5.1 Missing Retail Source File
 
**Behavior:** The `hdfsFileExists` check at ingestion detects the missing file and logs:
```
[ERROR] Retail dataset not found at hdfs://localhost:9000/retail/retail.csv
[ERROR] Upload it to HDFS with: hdfs dfs -put retail.csv /retail/retail.csv
```
The pipeline then calls `spark.stop()` and exits cleanly without a stack trace.
 
### 5.2 Missing GDP Source File
 
**Behavior:** The `withRetry` wrapper attempts ingestion up to 3 times with a 5-second
delay between attempts. If all attempts fail, the pipeline logs:
```
[WARN] GDP data could not be loaded. Continuing without GDP enrichment.
```
An empty GDP DataFrame is substituted, allowing the pipeline to continue. GDP-dependent
queries will return empty results but the pipeline does not crash, and the retail data
itself remains queryable.
 
### 5.3 Malformed Rows in Source Data
 
**Behavior:** The GDP parsing logic pads or trims each row to match the header length:
```scala
val padded = cols.padTo(headers.length, "").take(headers.length)
```
This ensures malformed rows with too few or too many columns do not throw an exception.
Rows with a non-numeric value in the `2011` GDP column are filtered out by the
`.filter(col("GDP_2011").isNotNull)` step after casting to `DoubleType`.
 
For the retail dataset, rows with null `CustomerID`, non-positive `Quantity` or
`UnitPrice`, or unparseable dates are removed during cleaning rather than causing
pipeline failure.
 
### 5.4 Empty Dataset After Cleaning
 
**Behavior:** After cleaning, the pipeline checks whether any rows remain:
```
[ERROR] All retail rows were filtered out during cleaning. Check source data quality.
```
The pipeline exits cleanly if the clean row count is zero, preventing downstream
queries from executing on an empty DataFrame.
 
### 5.5 Duplicate Data
 
**Behavior:** The pipeline does not explicitly deduplicate rows. The UCI dataset does
not contain exact duplicate transactions by design (each row represents a distinct
line item on an invoice). If duplicates were introduced via re-ingestion, revenue
aggregations would be inflated proportionally — a known limitation documented below.
 
### 5.6 HDFS Connection Failure
 
**Behavior:** If HDFS is not running when the pipeline starts, the `hdfsFileExists`
check catches the connection exception and logs a warning. The retail ingestion
`withRetry` block then attempts connection up to 3 times before logging an error
and exiting. The error message directs the user to start HDFS with `start-dfs.sh`.
 
---
 
## 6. Known Limitations
 
| Limitation | Impact | Mitigation |
|-----------|--------|------------|
| No explicit deduplication | Duplicate ingestion would inflate revenue totals | HDFS `-put -f` flag overwrites existing files, preventing accidental double-upload |
| Single-node memory bound | Pipeline fails at ~100M rows with OutOfMemoryError | Demonstrated in M2; inherent to single-node pseudo-distributed deployment |
| Static GDP data (2011 only) | GDP context does not reflect current economic conditions | Appropriate for the 2010–2011 UCI dataset timeframe |
| 3 unmappable countries | 1,052 rows excluded from GDP queries | Represents only 0.26% of clean data; not material to results |
| No streaming ingestion | Pipeline is batch-only | Outside project scope; noted as future improvement |
 
---
 
## 7. Performance Characteristics
 
| Metric | Value |
|--------|-------|
| Raw rows ingested | 541,909 |
| Clean rows processed | 397,884 |
| GDP rows loaded | 261 |
| Queries executed | 5 |
| Output format | Parquet (4 output directories) |
| Deployment | Single-node pseudo-distributed WSL environment |
| Spark shuffle partitions | 8 (tuned for single-node) |
 
**Query performance observations:**
- Queries 1–3 (revenue aggregation, market penetration, GDP tier) executed quickly
  with low shuffle overhead due to simple group-by aggregation patterns.
- Query 4 (monthly trend by GDP tier) required a three-way grouping and produced
  the highest shuffle overhead of the five queries.
- Query 5 (diagnostics) was the fastest, requiring a single full scan with
  conditional aggregation and no shuffle stage.
- The GDP file parsing stage (textFile + RDD map) is the slowest ingestion step
  due to row-by-row processing, but is a one-time cost that does not affect
  query performance
