
# Lab 3 – Data Preprocessing on Azure  
AICC6201 – Artificial Intelligence System Engineering  

This lab focuses on building a complete data preprocessing and ingestion pipeline on Microsoft Azure using a real-world Goodreads online books dataset. The goal is to simulate a multi-source data ingestion workflow for a modern data engineering architecture.

This README covers all required steps, including Azure Storage, Data Factory, Databricks preprocessing, Fabric transformations, and Homework Parts I & II.


# 1. Introduction

In this lab, we work with three Goodreads datasets:

- Books
- Authors
- Reviews

Each dataset is stored as JSON and includes nested fields that require careful extraction and cleaning. You will build a full ingestion pipeline using:

- Azure Storage (Raw + Processed)
- Azure Data Factory (Copy Data Flow)
- Azure Databricks (Data exploration & preprocessing)
- Microsoft Fabric (Dataflow Gen2 + Warehouse)

This lab prepares you for real-world preprocessing pipelines.

# 2. Architecture Overview

Local VM Data Generator
↓
Azure Storage (Blob)
├── Raw Zone
└── Processed Zone
↓
Azure Data Factory
├── Linked Services
├── Datasets
└── Copy Pipelines (JSON → Parquet)
↓
Azure Databricks
├── Schema Inspection
├── Cleaning (PERMISSIVE mode)
└── Merge / Parquet Write
↓
Microsoft Fabric
├── Dataflow Gen2
├── Lakehouse / Delta Tables
└── Warehouse + Power BI Dataset

---

# 3. Step A – Azure Storage Setup (Raw Layer)

1. Create a Storage Account (Data Lake Gen2 Enabled).
2. Create a container named:

lakehouse

3. Inside the container, create subdirectories:

lakehouse/raw/authors
lakehouse/raw/books
lakehouse/raw/reviews

4. A local VM continuously streams Goodreads JSON files using:

 Example commands (from lab PDF)
```bash
wget https://datarepo.../goodreads_books.json.gz
azcopy copy "books.json" "https://<account>.blob.core.windows.net/lakehouse/raw/books?<SAS>"
You should see multiple JSON files appearing automatically in Storage.
________________________________________
4. Step B – Generate Shared Access Signature (SAS)
1.	Navigate to the lakehouse container.
2.	Click Generate SAS.
3.	Enable at minimum:
•	Read
•	List
•	Write
4.	Note the generated SAS token.
This will be used in Data Factory and Databricks.
________________________________________
5. Step C – Azure Data Factory Setup (Copy Data Flow)
5.1 Create Linked Services
Create two linked services:
1. AzureBlobStorage_LS
•	Connects to your Storage Account (raw zone)
•	Uses SAS authentication
2. AzureBlobStorage_LS_Processed
•	Connects to processed zone
________________________________________
5.2 Create Datasets
You must create six datasets:
JSON (source)
•	authors_json
•	books_json
•	reviews_json
Parquet (sink)
•	authors_parquet
•	books_parquet
•	reviews_parquet
Each JSON dataset points to:
lakehouse/raw/<entity>
Each Parquet dataset outputs to:
lakehouse/processed/<entity>
________________________________________
5.3 Create Copy Pipeline
For each entity:
1.	Create a Copy Data activity.
2.	Select the JSON dataset as source.
3.	Select the Parquet dataset as sink.
4.	Enable:
o	Wildcard file matching
o	Recursive folder traversal
5.	Fix malformed JSON by enabling:
JSON Settings → Handling unquoted values
JSON Settings → Treat missing fields as null
6.	Run the pipeline and verify Parquet files appear under the processed folder.
________________________________________
6. Step D – Azure Databricks Preprocessing
6.1 Create Workspace, Cluster, and Notebook
1.	Create an Azure Databricks workspace.
2.	Configure access using Storage SAS.
3.	Create a cluster (recommended: Standard_DS3_v2).
4.	Create notebook:
goodreads_preprocessing
________________________________________
6.2 Mount Storage
Use the SAS key to access the lakehouse container:
spark.conf.set(
  "fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net",
  "SAS"
)

spark.conf.set(
  "fs.azure.sas.token.provider.type",
  "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)
________________________________________
6.3 Read JSON Raw Data
authors_raw = spark.read.json("abfss://lakehouse@<account>.dfs.core.windows.net/raw/authors")
books_raw   = spark.read.json("abfss://lakehouse@<account>.dfs.core.windows.net/raw/books")
reviews_raw = spark.read.json("abfss://lakehouse@<account>.dfs.core.windows.net/raw/reviews")
________________________________________
6.4 Inspect Schema
authors_raw.printSchema()
books_raw.printSchema()
reviews_raw.printSchema()
________________________________________
6.5 Cleaning Using PERMISSIVE Mode
clean_books = spark.read \
    .option("mode", "PERMISSIVE") \
    .json("abfss://lakehouse@<account>.dfs.core.windows.net/raw/books")
________________________________________
6.6 Explode Nested Fields
Especially for authors and books.
Example:
from pyspark.sql.functions import explode

df = books_raw.withColumn("author", explode("authors"))
________________________________________
6.7 Write Cleaned Data to Processed Layer
Always overwrite:
clean_books.write.mode("overwrite").parquet(
    "abfss://lakehouse@<account>.dfs.core.windows.net/processed/books"
)
Repeat the same for authors and reviews.
________________________________________
7. Step E – Microsoft Fabric (Dataflow Gen2)
7.1 Create Lakehouse
Inside Fabric:
1.	Create a Workspace.
2.	Create a Lakehouse resource.
3.	Go to Dataflow Gen2.
4.	Create a new dataflow for each dataset.
________________________________________
7.2 Load Files from Azure Storage
Use the SAS URL:
https://<account>.blob.core.windows.net/lakehouse/processed/books?<SAS>
________________________________________
7.3 Schema Transformations Required
For each dataset:
Books
•	Expand nested fields
•	Remove null entries
•	Ensure numeric types (e.g., ratings_count → Int64)
Authors
•	Flatten arrays
•	Fix malformed born_at, died_at fields
Reviews
•	Convert timestamp to datetime
•	Clean HTML tags
•	Convert rating to integer
________________________________________
7.4 Save Data as Delta Tables
1.	Save each transformation as a Lakehouse table.
2.	Publish a Warehouse.
3.	Create a Dataset for reporting.
________________________________________
8. Step F – Homework
Part I (Required)
Using Databricks:
1.	Load cleaned data.
2.	Convert timestamps properly.
3.	Replace HTML characters in review text.
4.	Remove invalid reviews.
5.	Reconstruct the dataset.
________________________________________
Part II (Required)
Using Fabric:
1.	Continue transformations from Part I.
2.	Write final Delta tables.
3.	Visualize summary using Power BI / Fabric.
________________________________________
9. Optional Section (From Lab Instructions)
If Fabric fails, you may complete ALL preprocessing in Databricks only, including:
•	Merging repeated authors
•	Handling corrupted review text
•	Timezone normalization
•	Writing final tables into processed/
________________________________________
10. Deliverables Checklist
Your Lab 3 submission must include:
•	✔ Raw & Processed folders created
•	✔ SAS key generated
•	✔ Linked Services created
•	✔ JSON → Parquet pipeline working
•	✔ Databricks cluster working
•	✔ Databricks preprocessing completed
•	✔ Fabric Dataflow Gen2 transformations
•	✔ Delta tables created
•	✔ Homework I & II completed
•	✔ Screenshots in GitHub repo
•	✔ README.md containing all steps
________________________________________
11. Repository Structure (Recommended)
LAB3/
│
├── images/                     # Screenshots (to be added by you)
├── notebooks/
│   └── goodreads_preprocessing.ipynb
├── sql/
│   └── any relevant SQL scripts
└── README.md
________________________________________
12. Author
Mohamed ElTayeb
Master’s – Artificial Intelligence & Cognitive Cybersecurity
Cybersecurity Engineer
GitHub: https://github.com/momo23121




