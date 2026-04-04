# Data Dictionary
 
## Scalable Retail Inventory Analytics — `retail_gdp` Schema
 
This document describes the schema of the unified `retail_gdp` dataset produced by the
multi-source retail analytics pipeline (`src/multi_source_retail_analytics.scala`). This dataset
is the primary analytical surface for all Spark SQL queries in Milestone 3.
 
The `retail_gdp` dataset is derived by joining two source datasets:
- **Source A:** UCI Online Retail Dataset (`/retail/retail.csv` in HDFS)
- **Source B:** World Bank GDP Dataset (`/retail/worldbank/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv` in HDFS)
 
---
 
## Field Reference
 
### Fields from Source A — UCI Online Retail Dataset
 
| Field | Type | Description | Notes |
|-------|------|-------------|-------|
| `InvoiceNo` | `String` | Unique identifier for each transaction invoice | Rows where `InvoiceNo` starts with `C` are cancellations and are removed during cleaning |
| `StockCode` | `String` | Unique identifier for each product | Used as the primary product key in product-level queries |
| `Description` | `String` | Human-readable product name | May be null for some records; not used as a join key |
| `Quantity` | `Integer` | Number of units purchased in the transaction | Rows with `Quantity <= 0` are removed during cleaning |
| `InvoiceDate` | `String` | Raw transaction timestamp as stored in the source file | Format: `yyyy/MM/dd HH:mm:ss`; retained as-is after parsing |
| `UnitPrice` | `Double` | Sale price per unit in GBP (£) | Cast from String to Double during cleaning; rows with `UnitPrice <= 0` are removed |
| `CustomerID` | `Integer` | Unique identifier for each customer | Rows with null `CustomerID` are removed during cleaning |
| `Country` | `String` | Country where the customer is located | Sometimes uses informal naming conventions (e.g., `EIRE`, `USA`); normalized via mapping table before joining |
 
---
 
### Fields Derived from Source A
 
| Field | Type | Derivation | Description |
|-------|------|------------|-------------|
| `Revenue` | `Double` | `Quantity * UnitPrice` | Total revenue for the transaction line in GBP (£) |
| `InvoiceDateParsed` | `Timestamp` | `to_timestamp(InvoiceDate, "yyyy/MM/dd HH:mm:ss")` | Parsed timestamp used to extract `Year` and `Month`; intermediate field |
| `Year` | `Integer` | `year(InvoiceDateParsed)` | Calendar year of the transaction; used in temporal trend queries |
| `Month` | `Integer` | `month(InvoiceDateParsed)` | Calendar month of the transaction (1–12); used in temporal trend queries |
 
---
 
### Fields from the Country Mapping Table
 
| Field | Type | Description | Notes |
|-------|------|-------------|-------|
| `WB_Country` | `String` | Normalized country name matching World Bank naming conventions | Derived by joining `Country` to an internal mapping table; null for unmappable entries (`European Community`, `Unspecified`, `Channel Islands`) |
 
---
 
### Fields from Source B — World Bank GDP Dataset
 
| Field | Type | Description | Notes |
|-------|------|-------------|-------|
| `GDP_2011` | `Double` | Country-level GDP for the year 2011 in current USD | Sourced from World Bank indicator `NY.GDP.MKTP.CD`; null for countries with no World Bank entry or no match in the mapping table; divide by `1e9` in queries to express in billions USD |
 
---
 
## Cleaning and Filtering Summary
 
The following transformations are applied to Source A before joining:
 
| Condition | Action | Reason |
|-----------|--------|--------|
| `CustomerID IS NULL` | Row removed | Unidentifiable customer; cannot be used in customer-level analysis |
| `InvoiceNo IS NULL` | Row removed | Unidentifiable transaction |
| `InvoiceNo LIKE 'C%'` | Row removed | Cancellation record; would distort revenue totals |
| `Quantity <= 0` | Row removed | Invalid or returned quantity |
| `UnitPrice <= 0` | Row removed | Invalid pricing record |
 
After cleaning, the dataset contains **397,884 rows** from an original raw count of **541,909 rows**.
 
---
 
## Join Specification
 
| Join | Type | Left Key | Right Key |
|------|------|----------|-----------|
| Retail → Country Mapping | Left | `Country` | `UCI_Country` |
| Retail → World Bank GDP | Left | `WB_Country` | `CountryName` |
 
Both joins are left joins to preserve all retail records regardless of GDP match status.
Rows without a GDP match (1,052 rows across 3 unmappable country values)
are retained in the dataset but excluded from GDP-dependent queries via
`WHERE GDP_2011 IS NOT NULL`.
 
---
 
## Country Mapping Notes
 
The following country name mismatches are resolved by the mapping table:
 
| UCI `Country` Value | World Bank `CountryName` Value |
|---------------------|-------------------------------|
| EIRE | Ireland |
| USA | United States |
| Czech Republic | Czechia |
| Hong Kong | Hong Kong SAR, China |
| RSA | South Africa |
 
The following UCI country values have no World Bank equivalent and are mapped to `null`:
 
| UCI `Country` Value | Reason |
|---------------------|--------|
| European Community | Supranational entity; no country-level GDP entry |
| Unspecified | No country information available |
| Channel Islands | British Crown dependency; not listed as a standalone World Bank country |
 
---
 
## Output Datasets
 
The pipeline writes the following Parquet datasets to HDFS under `/retail/output/`:
 
| Output Path | Contents | Key Fields |
|-------------|----------|------------|
| `revenue_by_country_gdp` | Total revenue and invoice counts per country, enriched with GDP | `Country`, `GDP_Billions_USD`, `TotalRevenue_GBP` |
| `revenue_per_gdp` | Revenue normalized by GDP as a market penetration proxy | `Country`, `TotalRevenue_GBP`, `RevenuePerBillionGDP` |
| `gdp_tier_analysis` | Purchasing behavior aggregated by GDP tier (High / Medium / Low) | `GDP_Tier`, `TotalRevenue_GBP`, `AvgRevenuePerCustomer` |
| `monthly_trend_by_gdp_tier` | Monthly revenue and invoice counts broken down by GDP tier | `Year`, `Month`, `GDP_Tier`, `MonthlyRevenue` |
