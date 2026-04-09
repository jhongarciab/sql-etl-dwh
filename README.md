# Data Warehouse and Analytics Project

## 📊 Project Overview

This project implements an end-to-end **Data Warehouse and Analytics solution** using PostgreSQL.
The goal is to integrate raw ERP and CRM data into a structured analytical environment following a **medallion architecture: Bronze → Silver → Gold** architecture.

The project focuses on:
- Data ingestion from multiple sources
- Data cleansing and standardization
- Business-ready analytical models
- SQL-based ETL orchestration using stored procedures

## ▶️ How to run (full pipeline)

This repository includes a single-entry pipeline runner that rebuilds the `DataWarehouse` database end-to-end.

```bash
./pipeline.sh -U <postgres_user>
```

What it does:
- Drops and recreates the `DataWarehouse` database (full reproducibility)
- Initializes schemas: `bronze`, `silver`, `gold`
- Bronze: creates tables, truncates, loads CSVs
- Silver: creates tables, runs `CALL silver.load_silver()`
- Gold: creates analytical views
- Runs quality checks for Silver and Gold

## 📄 Data contract

- `docs/data_contract.md` — minimum verifiable expectations (derived from DDL + procedures + tests)
- `docs/data_catalog.md` — gold layer column catalog

## 📂 Repository Structure

```text
sql-data-warehouse/
├── datasets/
├── docs/
│   ├── data_contract.md
│   ├── data_catalog.md
│   ├── data_flow.png
│   ├── data_integration.png
│   ├── data_model.png
│   └── naming_conventions.md
├── scripts/
│   ├── analytics/
│   └── db/
│       ├── init_db.sql
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── test/
│   ├── quality_silver.sql
│   ├── quality_gold.sql
│   └── proc_quality.sql
├── pipeline.sh
├── README.md
├── LICENSE
└── .gitignore
```
