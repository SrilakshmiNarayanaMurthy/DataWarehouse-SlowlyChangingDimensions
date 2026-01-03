# DataWarehouse-SlowlyChangingDimensions

🧊 Data Warehouse Implementation – Slowly Changing Dimensions (SCD) using Snowflake & dbt
📌 Project Overview

This project demonstrates the implementation of Slowly Changing Dimensions (SCD Types 1, 2, 3, and 4) using a modern cloud data-warehouse stack built on Snowflake, dbt (Data Build Tool), and Logstash.

The system is designed to efficiently handle changing customer data while:

Preserving historical records where required
Ensuring correctness of current data
Supporting incremental loading
Maintaining high performance and data quality

The project follows real-world data engineering best practices, including raw ingestion, structured transformation, dimensional modeling, testing, optimization, and modular design.

🎯 Objectives

Implement SCD Types 1, 2, 3, and 4 based on business needs
Ingest semi-structured data into Snowflake using Logstash
Transform raw JSON data into structured dimension tables
Use dbt incremental models to reduce processing cost
Enforce data quality using dbt tests
Maintain current and historical customer records efficiently

🏗️ Architecture Overview

End-to-End Data Flow
CSV (Faker Data)
   ↓
Logstash (CSV → JSON)
   ↓
Snowflake (RAW VARIANT table)
   ↓
Parsed / Structured Tables
   ↓
dbt Transformations
   ↓
SCD1 / SCD2 / SCD3 / SCD4 Tables

Key design principles:

Raw data stored unchanged
Transformations handled inside Snowflake
Business logic implemented using dbt models
History tracking varies by SCD type

| Component     | Purpose                                |
| ------------- | -------------------------------------- |
| **Snowflake** | Cloud data warehouse                   |
| **dbt**       | Data transformation, modeling, testing |
| **Logstash**  | Data ingestion (CSV → JSON)            |
| **Faker**     | Synthetic data generation              |
| **SQL**       | Transformation and SCD logic           |
| **YAML**      | Schema documentation & tests           |

📂 Repository Structure (Logical)

DataWarehouse-SlowlyChangingDimensions/
├── logstash/
│   └── logstash.conf
├── dbt_project/
│   ├── models/
│   │   ├── scd1_customers.sql
│   │   ├── scd2_customers.sql
│   │   ├── scd3_customers.sql
│   │   ├── scd4_customers_current.sql
│   │   └── scd4_customers_history.sql
│   ├── schema.yml
│   └── dbt_project.yml
├── data/
│   └── sample_customer_data.csv
└── README.md


Data Ingestion (Logstash → Snowflake)

Synthetic customer data generated using Faker
CSV files ingested via Logstash
Data converted to JSON format
Loaded into Snowflake as VARIANT data type

Raw Table
CUSTOMER_DATA.CUSTOMER_SCHEMA.CUSTOMERS_RAW
This table stores the full JSON payload in a single column (raw_data), preserving original source data.

🔄 Data Parsing & Structuring
Raw JSON data is extracted and cast into structured columns:

CUSTOMERS_PARSED
Key transformations:

JSON field extraction
Type casting (INT, STRING, BOOLEAN)
Standardized schema for downstream modeling
This table serves as the single source of truth for all SCD models.

🧠 Slowly Changing Dimension Implementations
🔹 SCD Type 1 – Overwrite (No History)

Updates overwrite existing records
No historical tracking
Suitable when history is not required
Example: Updating customer email replaces old value permanently.

Table:
SCD1_CUSTOMERS

🔹 SCD Type 2 – Full History Tracking

Inserts a new row on change
Maintains current_flag
Preserves complete audit history

Example:
Old record marked FALSE, new record marked TRUE.

Table:
SCD2_CUSTOMERS

🔹 SCD Type 3 – Limited History

Stores previous value in additional column
No new rows created
Tracks only most recent change

Example:
email → prev_email

Table:
SCD3_CUSTOMERS

🔹 SCD Type 4 – Hybrid (Current + History Tables)

Current table behaves like SCD1
History table stores all past versions
Best for separating operational queries from historical analysis

Tables:
SCD4_CUSTOMERS_CURRENT
SCD4_CUSTOMERS_HISTORY

⚡ Incremental Loading with dbt

All SCD models are implemented as incremental models, ensuring:
Only new or changed records are processed
Reduced warehouse cost
Faster pipeline execution
Key dbt features used:
is_incremental()
ref() for dependency management
Configurable materializations



✅ Data Quality & Testing

Implemented using dbt built-in tests and YAML configuration:
not_null
unique
accepted_values
current_flag validation
Failures are automatically surfaced during dbt test, ensuring data reliability.

🧯 Error Handling & Logging

dbt captures compilation and runtime errors
Incremental safety checks prevent duplicate processing
Logstash configuration avoids duplicate ingestion
Clear logs enable easy debugging and traceability
snowflake_project_GROUP4

🚀 Optimization Techniques

Incremental model design
Source-level filtering
Minimal joins
Modular SQL using CTEs
Separate materializations (tables vs views)
A dedicated view (latest_customers) provides only active records for fast querying.

🔁 Code Reusability & Modularity

Centralized source definitions
Reusable dbt models per SCD type
YAML-driven documentation & testing
Parameter-driven design for extensibility
This structure allows easy onboarding of new dimensions with minimal code changes.

▶️ How to Run the Project
# Install dbt
pip install dbt-snowflake

# Initialize dbt project
dbt init

# Run models
dbt run

# Run data quality tests
dbt test


Logstash pipeline should be running prior to dbt execution.

📌 Key Learnings

Practical implementation of all major SCD types
Real-world incremental data modeling
Snowflake + dbt best practices
Data quality enforcement in analytics engineering
Performance-aware warehouse design

🔮 Future Enhancements

CDC integration (Kafka / Debezium)
Snapshot-based SCD comparison
BI layer integration (Tableau / Power BI)
CI/CD automation for dbt
Row-level security implementation



