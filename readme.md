# YouTube Comments Sentiment Analysis Using Azure & Databricks

## Project Overview
This project aims to analyze real-time YouTube comments related to Azure technologies like Azure DevOps, Azure AI, and Azure Functions. The solution performs sentiment analysis to categorize user feedback as positive, neutral, or negative. The pipeline is implemented using a range of Azure services including Event Hub, Data Factory, Function Apps, and Databricks, and stores the results in Delta Lake tables.

---

## Architecture Overview
The architecture consists of the following steps:
1. **YouTube Data Fetching:** 
   - YouTube comments are fetched using the YouTube API.
   - The data is ingested in real-time using **Azure Event Hub**.

2. **Azure Data Factory (ADF) Pipeline:**
   - **ADF** orchestrates the end-to-end process, triggering the data ingestion at regular intervals.
   - It triggers an **Azure Function App** which fetches YouTube comments and sends them to Event Hub.
   - **ADF** then triggers a **Databricks notebook** to process the data.

3. **Data Processing and Sentiment Analysis:**
   - Azure **Databricks** is used to clean and transform the comments.
   - Sentiment analysis is applied using **VADER** to classify comments as positive, neutral, or negative.
   - Results are written to a **Delta Lake** table for further analysis and reporting.

4. **Delta Lake Storage:**
   - The processed data and sentiment scores are stored in Delta Lake, ensuring scalability and performance for querying large datasets.

---

## Key Components

### 1. **Azure Event Hub:**
   - Ingests YouTube comments in real-time as they are posted.

### 2. **Azure Data Factory (ADF):**
   - Triggers the ingestion and processing pipeline at scheduled intervals.
   - Manages incremental data fetching using the watermark pattern.

### 3. **Azure Function App:**
   - Fetches new comments based on the trigger from ADF.
   - Sends the comments to Event Hub.

### 4. **Azure Databricks:**
   - Handles data cleaning, transformation, and sentiment analysis using PySpark.
   - Implements VADER sentiment analysis as a UDF in Databricks.
   - Writes results to Delta Lake.

---

## Features

- **Real-Time Data Ingestion:** Ingests YouTube comments in real-time using Azure Event Hub.
- **Sentiment Analysis:** Classifies comments into positive, neutral, or negative sentiment using VADER.
- **Scalable Data Pipeline:** The pipeline can handle large volumes of data using Azure's scalable services.
- **Delta Lake for Storage:** Ensures efficient querying and data storage with Delta Lake.

---

## Setup & Deployment

### Prerequisites
- Azure Subscription
- YouTube Data API credentials
- Azure Databricks Workspace
- Azure Event Hub
- Azure Data Factory (ADF)

### Steps to Deploy

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/your-repository/youtube-sentiment-analysis.git
