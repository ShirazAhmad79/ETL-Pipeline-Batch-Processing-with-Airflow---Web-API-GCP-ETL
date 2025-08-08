# ETL Pipeline: Batch Processing with Airflow

This project demonstrates the implementation of an ETL (Extract, Transform, Load) pipeline using Apache Airflow, Google Cloud Composer, Google Cloud Storage (GCS), BigQuery, and Cloud Functions. The pipeline automates the process of extracting trading data, transforming it, and loading it into BigQuery for analysis.

## 📁 Project Structure

```
ETL-pipeline-Batch-processing-with-Airflow/
├── dag.py
├── fetch_data.py
├── metaData/
│   ├── bq.json
│   └── udf.js
├── cloud run function/
│   ├── main.py
│   └── requirments.txt
├── tradingData.csv
└── README.md
```

* **dag.py**: Defines the Airflow DAG that orchestrates the ETL workflow.
* **fetch\_data.py**: Contains the logic to fetch and prepare trading data for processing.
* **metaData/**: Holds metadata files:

  * **bq.json**: Defines the BigQuery table schema.
  * **udf.js**: Contains JavaScript UDFs for data transformation.
* **cloud run function/**: Holds logic to pull csv from bucket via event trigger when files are uploaded:

  * **main.py**: Picks up every file uploaded by DAG and process it to create a dataflow job
  * **requirments.txt**: Contains package dependencies to run the main.py.
* **tradingData.csv**: Sample trading data used for testing the pipeline.
* **README.md**: Provides an overview and instructions for the project.

## 🚀 Workflow Overview

1. **Data Ingestion**: The pipeline is triggered by the presence of `tradingData.csv` in a designated GCS bucket.
2. **Data Transformation**: A Cloud Function is invoked to launch a Dataflow job that applies transformations using the provided UDFs.
3. **Data Loading**: The transformed data is loaded into a BigQuery table as defined in `bq.json`.
3. **Dashboard**: The data loaded into the BigQuery table is further utilized to prepare data dashboards for different stackholders.

## 🛠️ Technologies Used

* **Apache Airflow**: Orchestrates the ETL workflow.
* **Google Cloud Composer**: Managed Airflow service for scheduling and monitoring.
* **Google Cloud Storage (GCS)**: Stores input and processed data files.
* **Google Cloud Run Functions**: Executes serverless functions in response to events.
* **Google Cloud Dataflow**: Processes and transforms data at scale.
* **BigQuery**: Data warehouse for storing and analyzing processed data.

## ⚙️ Setup Instructions

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/srbsnkr/ETL-pipeline-Batch-processing-with-Airflow.git
   cd ETL-pipeline-Batch-processing-with-Airflow
   ```

2. **Configure GCS Bucket**:

   * Create a GCS bucket (if not already existing).
   * Upload `tradingData.csv` to the bucket to trigger the pipeline.

3. **Deploy Cloud Function**:

   * Ensure the Cloud Function is set up to respond to GCS events.
   * The function should launch a Dataflow job using the provided `udf.js` and `bq.json`.

4. **Set Up Airflow DAG**:

   * Deploy `dag.py` to your Cloud Composer environment.
   * Ensure all necessary connections and variables are configured in Airflow.

5. **Run the Pipeline**:

   * Once everything is set up, to test upload a new `tradingData.csv` file to GCS will automatically trigger the pipeline. Further wait or manually trigger the Airflow DAG to run the ETL process.

## 📌 Notes

* Ensure that all Google Cloud services (Cloud Run Functions, Dataflow, BigQuery) have the necessary permissions to interact with each other.
* Monitor the Airflow UI for DAG execution status and logs.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

For any questions or issues, please open an issue in the repository.
