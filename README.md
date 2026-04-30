Entity Resolution Autoloader Pipeline - Architecture Diagram
This project implements a smart data pipeline that automatically loads entity data files from S3 into Delta Lake tables. The pipeline is designed to handle real-world data challenges like duplicate records and data quality issues while maintaining complete audit trails.

Architecture Diagram:
•	- Files are loaded from the S3 bucket.
•	- The autoloader pipeline discovers tables and lists files.
•	- Each file is processed: data is read and transformed (handling VOID columns and type issues).
•	- If unique keys are found, a MERGE is performed; otherwise, the table is OVERWRITTEN.
•	- Change Data Feed (CDF) is enabled.
•	- Processed files are archived.
•	- A report and audit trail are generated.
•	- Data lands in Delta Lake tables (bronze).



## 📊 Autoloader Ingestion Pipeline Flow (Horizontal View)

```mermaid
flowchart LR
    A["S3 Bucket: entity_full/"] --> B["Autoloader Pipeline"]
    B --> C["Table Discovery (reads Delta Lake catalog)"]
    B --> D["File Listing (finds .txt files)"]
    D --> E["File Processing Loop"]
    E --> F["Read & Transform Data (handles VOID columns, type issues)"]
    F --> G{"Unique Keys Found?"}
    G -- Yes --> H["MERGE into Delta Table"]
    G -- No --> I["OVERWRITE Delta Table"]
    H --> J["Enable Change Data Feed (CDF)"]
    I --> J
    J --> K["Archive Processed File (S3 archive)"]
    K --> L["Generate Report & Audit Trail"]
    L --> M["Delta Lake Tables (bronze)"]
