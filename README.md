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
