# Lab 3: Spark-Based Data Lake and Analysis Pipelines

**Course**: 23D020 - Big Data Management for Data Science  
**Project**: Lab 3 - Data Management and Analysis Backbones  
**Team Members**:

- Marvin Ernst
- Oriol Gelabert
- Alex Malo

Team: **L3-T04**

---

## Project Structure

This repository contains the full implementation of our Lab 3 project. We implemented both the **Data Management Backbone** and **Data Analysis Backbone** using Apache Spark and Jupyter notebooks.

```
Big-Data-Lab-3/
├── landing_zone/
├── formatted_zone/
├── exploitation_zone/
├── notebooks/
│   ├── scripts/
│   │   ├── 01_data_formatting_pipeline.py
│   │   ├── 02_exploitation_pipeline.py
│   │   └── 03_analysis_pipeline.py
│   ├── 01_data_formatting_pipeline.ipynb
│   ├── 02_exploitation_pipeline.ipynb
│   └── 03_analysis_pipeline.ipynb
├── airflow/
│   └── lab3_dag.py
├── OriginalData/
├── Data_Manual_unzipped/
├── pyproject.toml
├── poetry.lock
└── README.md
```

Note that `OriginalData` are the zipped original sources (ignored in Git) and `Data_Manual_unzipped`are the manually extracted files (not used directly).

- we save our outputs in Jupyter notebooks
- we have separate folders for landing, formatting, and exploitation zones
- all pipeline steps are reproducible via standalone `.py` scripts and can be orchestrated

---

## Summary

### Objectives

- Set up a data lake with three structured zones: **Landing**, **Formatted**, and **Exploitation**
- Perform descriptive analysis using Spark, computing key aggregations and indicators
- Implement data quality and reproducibility practices across the Spark pipeline
- BONUS: Use Apache Airflow to orchestrate all three pipeline steps

---

## Pipelines Overview

### 1. Data Formatting (`01_data_formatting_pipeline.py`)

- Unzips raw `.csv.zip` files from the Landing Zone
- Loads three heterogeneous datasets:
  - Tourist housing data (`*.csv`)
  - Commercial premises (`*.csv`)
  - Household size data (`*.json`)
- Cleans, normalizes, and writes to the Formatted Zone in Parquet format

### 2. Exploitation Pipeline (`02_exploitation_pipeline.py`)

- Aggregates and enriches data from the Formatted Zone
- Calculates indicators by district, neighborhood, and census section
- Stores KPI-ready tables into the Exploitation Zone

### 3. Analysis Pipeline (`03_analysis_pipeline.py`)

- Performs descriptive analysis (EDA) using aggregated data
- Visualizes household sizes, coworking/nightlife shares, and tourist density
- Provides comparative dashboards with summary tables and time trends

---

## Selected Datasets

We work with the following three datasets, all located in the `landing_zone/`:

- **Tourist Housing**: quarterly `.csv` files containing apartment registry data
- **Commercial Premises**: yearly `.csv` files about ground-floor businesses
- **Household Size**: `.json` data on the number of people per dwelling unit

At least one dataset is in JSON format, satisfying the project requirement.

---

## Technologies Used

- **Apache Spark** (via PySpark)
- **Jupyter** for development and EDA
- **Seaborn** / **Matplotlib** for visualizations
- **Poetry** for dependency management
- **Pandas** (for dashboarding)
- **Apache Airflow** for orchestration of the pipelines

---

## Orchestration (Bonus Task C)

We implemented a simple Apache Airflow DAG (`lab3_dag.py`) that sequentially runs the three pipelines using `PythonOperator`. The DAG uses the script files in `notebooks/scripts/` and resolves all paths dynamically.

This satisfies the bonus requirement of using orchestration for reproducible execution. We do not include Airflow logs or screenshots, but the DAG is fully deployable.
