#!/usr/bin/env python

"""
Lab 3 - Data Analysis Pipeline

Group: L3-T04 - Marvin Ernst, Oriol Gelabert, Alex Malo

This script implements the third step of the Data Management Backbone for Lab 3:
Perform descriptive analysis and generate dashboards based on aggregated data
in the Exploitation Zone using matplotlib and seaborn.
"""

# Setup
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from glob import glob

# Create Spark session
spark = SparkSession.builder.appName("Analysis Pipeline").getOrCreate()

# Define paths
project_root = Path(__file__).resolve().parent.parent
exploitation = project_root / "exploitation_zone"

# Load datasets
df_household_dis = spark.read.parquet(f"{exploitation}/households_districte_2022.parquet")
pdf_household_dis = df_household_dis.toPandas()

df_com_dis = spark.read.parquet(f"{exploitation}/comercial_indicators_district_2022.parquet")
pdf_com_dis = df_com_dis.toPandas()

df_hut_dis = spark.read.parquet(f"{exploitation}/hut_district_2022_1T.parquet")
pdf_hut_dis = df_hut_dis.toPandas()

# Household Size Distribution
plt.figure(figsize=(12, 6))
sns.barplot(data=pdf_household_dis, x="DES_DISTRICTE", y="NUM_VALOR", hue="NUM_PERSONES_AGG")
plt.title("Household Size Distribution by District (2022)")
plt.xlabel("District")
plt.ylabel("Number of Households")
plt.xticks(rotation=35)
plt.legend(title="Household Size", loc='upper center', bbox_to_anchor=(0.5, -0.4), ncol=6, frameon=False)
plt.tight_layout()
plt.show()

# Commercial Indicators Barplot
pdf_com_dis["PCT_IND_COWORKING"] = (pdf_com_dis["TOTAL_IND_COWORKING"] / pdf_com_dis["TOTAL"]) * 100
pdf_com_dis["PCT_IND_OCI_NOCTURN"] = (pdf_com_dis["TOTAL_IND_OCI_NOCTURN"] / pdf_com_dis["TOTAL"]) * 100

plt.figure(figsize=(10, 5))
sns.barplot(data=pdf_com_dis, x="COD_DISTRICTE", y="PCT_IND_COWORKING", color="skyblue", label="Coworking")
sns.barplot(data=pdf_com_dis, x="COD_DISTRICTE", y="PCT_IND_OCI_NOCTURN", color="coral", label="Nightlife", alpha=0.6)
plt.title("Coworking vs Nightlife Premises by District (2022)")
plt.xlabel("District Code")
plt.ylabel("Percentage of Total Premises")
plt.legend()
plt.tight_layout()
plt.show()

# Tourist Licenses by District
plt.figure(figsize=(10, 5))
sns.barplot(data=pdf_hut_dis.sort_values("TOTAL", ascending=False), x="COD_DISTRICTE", y="TOTAL", palette="Blues_d")
plt.title("Number of Tourist Licenses by District (Q1 2022)")
plt.xlabel("District Code")
plt.ylabel("Number of Licenses")
plt.tight_layout()
plt.show()

# Scatterplots: Tourism vs Commercial Indicators
pdf_com_dis["COD_DISTRICTE"] = pdf_com_dis["COD_DISTRICTE"].astype(int)
pdf_hut_dis["COD_DISTRICTE"] = pdf_hut_dis["COD_DISTRICTE"].astype(int)

merged_df = pdf_hut_dis.merge(
    pdf_com_dis[["COD_DISTRICTE", "PCT_IND_COWORKING", "PCT_IND_OCI_NOCTURN"]],
    on="COD_DISTRICTE", how="left"
)

plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
sns.scatterplot(data=merged_df, x="TOTAL", y="PCT_IND_COWORKING")
plt.title("Tourist Licenses vs % Coworking")
plt.xlabel("Total Tourist Licenses")
plt.ylabel("% Coworking Premises")

plt.subplot(1, 2, 2)
sns.scatterplot(data=merged_df, x="TOTAL", y="PCT_IND_OCI_NOCTURN")
plt.title("Tourist Licenses vs % Nightlife")
plt.xlabel("Total Tourist Licenses")
plt.ylabel("% Nightlife Premises")

plt.tight_layout()
plt.show()

# Time Trends of Tourist Housing
hut_files = sorted(glob(f"{exploitation}/hut_district_20*_*.parquet"))

dfs = []
for file in hut_files:
    name = os.path.basename(file).replace("hut_district_", "").replace(".parquet", "")
    year, quarter = name.split("_")
    df_temp = spark.read.parquet(file).toPandas()
    df_temp["YEAR"] = int(year)
    df_temp["QUARTER"] = quarter
    dfs.append(df_temp)

df_hut_time = pd.concat(dfs, ignore_index=True)
df_hut_time["QUARTER"] = pd.Categorical(df_hut_time["QUARTER"], categories=["1T", "2T", "3T", "4T"], ordered=True)
df_hut_time["TIME"] = df_hut_time["YEAR"].astype(str) + "_" + df_hut_time["QUARTER"].astype(str)

plt.figure(figsize=(12, 6))
sns.lineplot(data=df_hut_time, x="TIME", y="TOTAL", hue="COD_DISTRICTE", marker="o")
plt.title("Tourist Licenses Over Time by District")
plt.xlabel("Time (Year_Quarter)")
plt.ylabel("Number of Licenses")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Histogram of Tourist License Distribution
plt.figure(figsize=(8, 5))
sns.histplot(pdf_hut_dis["TOTAL"], bins=20, kde=True)
plt.title("Distribution of Tourist Licenses per District (Q1 2022)")
plt.xlabel("Number of Licenses")
plt.ylabel("Frequency")
plt.tight_layout()
plt.show()

# KPI Summary Table
avg_household = pdf_household_dis.groupby("COD_DISTRICTE").apply(
    lambda g: (g["NUM_PERSONES_AGG"] * g["NUM_VALOR"]).sum() / g["NUM_VALOR"].sum()
).reset_index(name="AVG_HOUSEHOLD_SIZE")

df_kpi = pdf_hut_dis.merge(avg_household, on="COD_DISTRICTE", how="left").merge(
    pdf_com_dis[["COD_DISTRICTE", "TOTAL", "TOTAL_IND_COWORKING", "TOTAL_IND_OCI_NOCTURN"]],
    on="COD_DISTRICTE", how="left"
)

df_kpi = df_kpi.rename(columns={"TOTAL_x": "NUM_TOURIST_LICENSES", "TOTAL_y": "NUM_COMMERCIAL_PREMISES"})

# Print full KPI table
pd.set_option("display.max_rows", None)
print(df_kpi[["COD_DISTRICTE", "NUM_TOURIST_LICENSES", "NUM_COMMERCIAL_PREMISES",
              "TOTAL_IND_COWORKING", "TOTAL_IND_OCI_NOCTURN", "AVG_HOUSEHOLD_SIZE"]].sort_values("NUM_TOURIST_LICENSES", ascending=False))

# Final confirmation
print("Analysis pipeline executed successfully.")