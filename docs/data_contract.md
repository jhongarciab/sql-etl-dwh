# Data Contract — SQL Data Warehouse (Bronze → Silver → Gold)

This document defines the **minimum, verifiable expectations** for the datasets, schemas, tables, and transformations implemented in this repository.

It is **derived strictly from the repository’s source-of-truth files**:
- DDL: `scripts/db/*/ddl_*.sql`, `scripts/db/init_db.sql`
- Transformations: `scripts/db/silver/proc_silver.sql`
- Quality checks: `test/quality_silver.sql`, `test/quality_gold.sql`
- Gold catalog: `docs/data_catalog.md`

No additional business assumptions are introduced beyond what is encoded in the SQL.

---

## 1) Scope

### Layers
- **Bronze**: raw/staging tables loaded from external CSV files.
- **Silver**: cleansed/standardized tables produced from Bronze.
- **Gold**: business-facing analytical views (star schema) built from Silver.

### Schemas
Created by `scripts/db/init_db.sql`:
- `bronze`
- `silver`
- `gold`

---

## 2) Inputs (Bronze load contract)

### Source systems
As documented in `docs/naming_conventions.md`, Bronze and Silver tables are prefixed by source system:
- `crm_*`
- `erp_*`

### Bronze tables (structure)
Defined in `scripts/db/bronze/ddl_bronze.sql`.

CRM tables:
- `bronze.crm_cust_info`
- `bronze.crm_prd_info`
- `bronze.crm_sales_details`

ERP tables:
- `bronze.erp_cust_az12`
- `bronze.erp_loc_a101`
- `bronze.erp_px_cat_g1v2`

### Bronze load mechanism
Defined in `scripts/db/bronze/load_insert.sql`.

- Uses `\COPY` from local paths under `./datasets/` into the Bronze tables.
- Because `\COPY` is a `psql` command, it is executed via a `psql` client context (not inside stored procedures).

---

## 3) Silver transformation contract

### Silver tables (structure)
Defined in `scripts/db/silver/ddl_silver.sql`.

CRM tables:
- `silver.crm_cust_info`
- `silver.crm_prd_info`
- `silver.crm_sales_details`

ERP tables:
- `silver.erp_cust_az12`
- `silver.erp_loc_a101`
- `silver.erp_px_cat_g1v2`

Technical column convention
- Silver tables include a technical timestamp column: `dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP`.

### Silver ETL entrypoint
Defined in `scripts/db/silver/proc_silver.sql`:
- Stored procedure: `silver.load_silver()`

`silver.load_silver()` performs:
- `TRUNCATE` of Silver tables.
- `INSERT INTO ... SELECT ...` from Bronze with standardization and validation rules.

### Silver standardization rules (verbatim behaviors)
Derived from `scripts/db/silver/proc_silver.sql`.

#### Customers (CRM → Silver)
- Trims: `cst_firstname`, `cst_lastname`.
- Standardizes:
  - `cst_marital_status`: `M → Married`, `S → Single`, else `NaN`.
  - `cst_gndr`: `F → Female`, `M → Male`, else `NaN`.
- Deduplication rule: keep the latest record per `cst_id` by `cst_create_data DESC` (`ROW_NUMBER() ... WHERE flag_last = 1`).

#### Products (CRM → Silver)
- `cat_id` is derived from `prd_key`:
  - `REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')`
- `prd_key` is derived from the tail of the original key:
  - `SUBSTRING(prd_key, 7, LENGTH(prd_key))`
- Product line standardization (`prd_line`):
  - `M → Mountain`, `R → Road`, `S → Other Sales`, `T → Touring`, else `NaN`.
- `prd_end_dt` is derived using a window function:
  - `LEAD(prd_start_dt) ... - 1`
- `prd_cost` is loaded as-is from Bronze (`prd_cost`), i.e., it may be `NULL`.

#### Sales details (CRM → Silver)
- Date parsing from integer-like fields to `DATE`:
  - If value is `0` or its string length is not 8, the date becomes `NULL`.
  - Else: `TO_DATE(value::TEXT, 'YYYYMMDD')`.
- Sales amount rule:
  - If `sls_sales` is `NULL`, `<= 0`, or inconsistent with `sls_quantity * ABS(sls_price)`, then set
    `sls_sales = sls_quantity * ABS(sls_price)`.
- Price rule:
  - If `sls_price` is `NULL` or `<= 0`, set
    `sls_price = COALESCE(sls_sales / NULLIF(sls_quantity, 0), 0)`.

#### ERP customers
- `cid`: if starts with `NAS`, strip prefix (`SUBSTRING(cid, 4, ...)`).
- `bdate`: out-of-range dates (< 1925-01-01 or > current_date) become `NULL`.
- `gen` standardization:
  - `F/FEMALE → Female`, `M/MALE → Male`, else `NaN`.

#### ERP locations
- `cid`: remove dashes: `REPLACE(cid, '-', '')`.
- `cntry` standardization:
  - `DE → Germany`
  - `US/USA → United States`
  - `NULL/empty → NaN`
  - else trimmed value.

#### ERP categories
- Loaded as-is from Bronze for columns: `id`, `cat`, `subcat`, `maintenance`.

---

## 4) Gold layer contract

### Gold objects
Defined in `scripts/db/gold/ddl_gold.sql` as **views**:
- `gold.dim_customers`
- `gold.dim_products`
- `gold.fact_sales`

The Gold column-level descriptions are cataloged in `docs/data_catalog.md`.

### Gold model (star schema intent)
Gold is implemented as a star schema via:
- Dimensions: `dim_customers`, `dim_products`
- Fact: `fact_sales`

### Gold joins (as implemented)
From `scripts/db/gold/ddl_gold.sql`:
- `gold.dim_customers` joins:
  - `silver.crm_cust_info ci`
  - left join `silver.erp_cust_az12 ca ON ci.cst_key = ca.cid`
  - left join `silver.erp_loc_a101 la ON ci.cst_key = la.cid`
- `gold.dim_products` joins:
  - `silver.crm_prd_info pn`
  - left join `silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id`
  - filter: `WHERE pn.prd_end_dt IS NULL` (active products)
- `gold.fact_sales` joins:
  - `silver.crm_sales_details sd`
  - inner join `gold.dim_products pr ON sd.sls_prd_key = pr.product_number`
  - inner join `gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id`

### Grain (granularity) — stated minimally
Because `gold.fact_sales` is a view over `silver.crm_sales_details` with joins to dimensions, the resulting grain is:
- **one row per row in `silver.crm_sales_details` that successfully matches a product and a customer**
  (due to the `INNER JOIN` conditions).

This is the strictest statement supported by the SQL without introducing assumptions about order-line uniqueness.

---

## 5) Quality / sanity checks (verifiable acceptance criteria)

### Silver checks
Defined in `test/quality_silver.sql`.

At minimum, the repo includes checks for:
- Duplicate/null primary keys (by grouping and null checks) on:
  - `silver.crm_cust_info (cst_id)`
  - `silver.crm_prd_info (prd_id)`
- Unwanted whitespace in key string fields (TRIM comparison).
- Value standardization inspection via `SELECT DISTINCT` (e.g., gender, marital status, product line, countries, categories).
- Product date order validity:
  - `prd_end_dt < prd_start_dt`.
- Sales date validity checks:
  - Bronze raw format/values (`YYYYMMDD` as int)
  - Silver converted `DATE` nulls
  - Order date ordering constraints (`order > ship` or `order > due`).
- Sales business rule consistency:
  - `sls_sales == sls_quantity * sls_price` and positivity constraints.
- ERP customer birthdate range validation.

### Gold checks
Defined in `test/quality_gold.sql`.

At minimum, the repo includes checks for:
- Uniqueness of surrogate keys in:
  - `gold.dim_customers (customer_key)`
  - `gold.dim_products (product_key)`
- Referential integrity of the fact model by checking null joins:
  - `gold.fact_sales` left-joined to both dimensions to identify missing keys.

---

## 6) Contract change policy (minimal)

- **Schema changes** are governed by the DDL files under `scripts/db/`.
- **Transformation rule changes** are governed by `scripts/db/silver/proc_silver.sql`.

For any change that affects Gold outputs, update:
- the relevant DDL/procedure/test file(s), and
- `docs/data_catalog.md` if column semantics change.
