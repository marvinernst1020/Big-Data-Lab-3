# Lab 3: Spark-Based Data Lake & Analysis Pipelines

**Course**: 23D020 - Big Data Management for Data Science  
**Project**: Lab 3 - Data Management & Analysis Backbones  
**Team Members**:  
- Marvin Ernst  
- Oriol Gelabert
- Alex Malo

---

## Project Structure

This repository contains the full implementation of our Lab 3 project. We implemented both the **Data Management Backbone** and **Data Analysis Backbone** using Apache Spark.

- we save our outputs in Jupyter notebooks
- we have seperate folder for landing and formatiing zone

---

## Summary

### Objectives
- Implement a data lake with three structured zones (Landing, Formatted, Exploitation)
- Perform either **Descriptive** or **Predictive** analysis using Spark
- Follow best practices in data validation, model training, and reproducibility

---

## Pipelines Overview

### 1. Data Formatting (`01_data_formatting.py`)
- Load 3 raw datasets (at least one JSON) from the Landing Zone
- Clean and standardize schemas
- Write partitioned data to the Formatted Zone in Parquet or Delta format

### 2. Exploitation Pipeline (`02_exploitation_pipeline.py`)
- Load data from Formatted Zone
- Join/enrich data, engineer features, clean for analysis
- Store results in CSV, Parquet, or Delta format in Exploitation Zone

### 3. Analysis Pipeline (`03_analysis_pipeline.py`)
- (Option A) Perform EDA and visualize with Seaborn/Matplotlib
- (Option B) Train & evaluate ML models with Spark MLlib
- Track models using MLflow and select best one for deployment

---

## Selected Datasets

- [Dataset 1] (e.g., `income_data.json`)
- [Dataset 2] (e.g., `idealista_data.csv`)
- [Dataset 3] (e.g., `unemployment_data.csv`)

At least one dataset is in JSON format, as required. See `assumptions.pdf` for full details.

---

## ⚙Technologies Used

- Apache Spark (PySpark)
- Pandas / Seaborn / Matplotlib (for EDA)
- Spark MLlib (for ML tasks)
- MLflow (for model tracking)
- Optional: Apache Airflow (orchestration)

---

## How to Run

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the notebooks/scripts in order:
   ```bash
   python notebooks/01_data_formatting.py
   python notebooks/02_exploitation_pipeline.py
   python notebooks/03_analysis_pipeline.py
   ```



