#!/usr/bin/env python

"""
Lab 3 - Data Formatting Pipeline

Group: L3-T04 - Marvin Ernst, Oriol Gelabert, Alex Malo

This script implements the first step of the Data Management Backbone for Lab 3: transforming raw datasets in the Landing Zone into cleaned, standardized datasets saved in the Formatted Zone using PySpark.

Steps:
1. Data Ingestion and Exploration
2. Data Cleaning and Standardization
3. Formatted Output to Parquet
"""

from pathlib import Path
import os
import zipfile
import re
import warnings
warnings.simplefilter("ignore", category=FutureWarning)

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, to_date, concat, lit, substring, when, regexp_replace, sum as _sum, round, count, create_map, lower, trim
from pyspark.sql.types import StringType
from unidecode import unidecode

# Initialize Spark:
appName = "Lab3_Format_Pipeline"
master = "local[*]"
conf = SparkConf().setAppName(appName).setMaster(master).set("spark.driver.memory", "4g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Set up paths
project_root = Path(__file__).resolve().parent.parent
landing = project_root / "landing_zone"
formatted = project_root / "formatted_zone"
formatted.mkdir(parents=True, exist_ok=True)

# Unzip files 
for file in landing.glob("*.zip"):
    try:
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(landing)
            print(f"Extracted: {file.name} to {landing}")
        file.unlink()
        print(f"Deleted ZIP: {file.name}")
    except zipfile.BadZipFile:
        print(f"Skipped (not a valid ZIP): {file.name}")

# Define helper UDFs 
unidecode_udf = udf(lambda x: unidecode(x.lower()) if x else None, StringType())
normalize_udf = udf(lambda x: unidecode(x.lower().strip()) if x else "", StringType())

def parse_boolean_indicator(column):
    return when(normalize_udf(col(column)).isin("si", "yes", "true", "1"), 1).otherwise(0)

# Load mappings from tourist housing to extract district/barri codes 
df_tourist_23 = spark.read.option("header", True).csv(f"{landing}/2023_1T_hut_comunicacio.csv")
district_dict = {row["DISTRICTE"]: row["CODI_DISTRICTE"] for row in df_tourist_23.select("CODI_DISTRICTE", "DISTRICTE").distinct().collect()}
neighborhood_dict = {row["BARRI"]: row["CODI_BARRI"] for row in df_tourist_23.select("CODI_BARRI", "BARRI").distinct().collect()}

district_map_expr = create_map(*[item for k, v in district_dict.items() for item in (lit(k), lit(v))])
neighbourhood_map_expr = create_map(*[item for k, v in neighborhood_dict.items() for item in (lit(k), lit(v))])

# We match files using the pattern "YYYY_QT", e.g., "2020_1T":
pattern = re.compile(r"(20\d{2})_(\dT)", re.IGNORECASE)

# We define a dictionary to store the cleaned DataFrames for each year and quarter:
df_dict = {}

# Then we iterate through all files in the landing zone:
for file in os.listdir(landing):
    match = pattern.match(file)
    if match:
        year = match.group(1)
        quarter = match.group(2)
        df_name = f"hut_{year}_{quarter}"
        full_path = os.path.join(landing, file)

        input_path = full_path
        temp_path = full_path + ".tmp"

        # For files from early 2020, we use ISO encoding and fix delimiters and location names:
        if year == '2020' and (quarter in ['1T', '2T']):
            with open(input_path, "r", encoding="iso-8859-1") as f_in, open(temp_path, "w", encoding="iso-8859-1") as f_out:
                for i, line in enumerate(f_in):
                    line_replaced = line.replace(";", ",")
                    line_replaced = line_replaced.replace(",Sant Pere, Santa Caterina i la Ribera", ',"Sant Pere, Santa Caterina i la Ribera"')
                    line_final = line_replaced.replace(",Vallvidrera, el Tibidabo i les Planes", ',"Vallvidrera, el Tibidabo i les Planes"')
                    f_out.write(line_final)
        else:
            # For other files, we apply UTF-8 and fix common formatting issues in 2023:
            with open(input_path, "r", encoding="UTF-8") as f_in, open(temp_path, "w", encoding="UTF-8") as f_out:
                for i, line in enumerate(f_in):
                    if i == 0 and year == '2023' and (quarter == '1T' or quarter == '3T'):
                        line = line.replace("LATITUD_Y", "LATITUD_Y\n")
                    line_replaced = line.replace(";", ",")
                    line_replaced = line_replaced.replace(",Sant Pere, Santa Caterina i la Ribera", ',"Sant Pere, Santa Caterina i la Ribera"')
                    line_final = line_replaced.replace(",Vallvidrera, el Tibidabo i les Planes", ',"Vallvidrera, el Tibidabo i les Planes"')
                    f_out.write(line_final)

        # We replace the original file with the cleaned temporary version:
        os.replace(temp_path, input_path)

        try:
            # We load the cleaned CSV file using the correct encoding:
            if year == '2020' and (quarter in ['1T', '2T']):
                df = spark.read.option("header", True).option("encoding", "iso-8859-1").csv(full_path)
            else:
                df = spark.read.option("header", True).option("encoding", "UTF-8").csv(full_path)

            # We rename inconsistent column names across files:
            df = df.withColumnsRenamed({
                'BARRI': 'DES_BARRI',
                'TIPUS_CARRER': 'COD_TIPUS_CARRER',
                'CARRER': 'DES_CARRER',
                'LLETRA1': 'DES_LLETRA_1',
                'LLETRA2': 'DES_LLETRA_2',
                'BLOC': 'DES_BLOC',
                'PORTAL': 'DES_PORTAL',
                'ESCALA': 'DES_ESCALA',
                'PIS': 'DES_PIS',
                'PORTA': 'DES_PORTA',
                'NOM_BARRI': 'DES_BARRI',
                'NOM_DISTRICTE': 'DES_DISTRICTE',
                'DISTRICTE': 'DES_DISTRICTE',
                'NUMERO_REGISTRE_GENERALITAT': 'NUM_REGISTRE',
                'NUMERO_PLACES': 'NUM_PLACES',
                'N_EXPEDIENT': 'ID_EXPEDIENT'
            })

            # We cast the street number fields to integers and drop the originals:
            df = df.withColumn("COD_TIPUS_NUM", col("TIPUS_NUM").cast("int")) \
                   .withColumn("NUM_CARRER_1", col("NUM1").cast("int")) \
                   .withColumn("NUM_CARRER_2", col("NUM2").cast("int")) \
                   .drop("NUM1", "NUM2", "TIPUS_NUM")

            # If coordinate columns exist, we convert them to float and fix decimal format:
            if "LONGITUD_X" in df.columns and "LATITUD_Y" in df.columns:
                df = df.withColumn("NUM_LONGITUD_X", regexp_replace(col("LONGITUD_X"), ",", ".").cast("float")) \
                       .withColumn("NUM_LATITUD_Y", regexp_replace(col("LATITUD_Y"), ",", ".").cast("float")) \
                       .drop("LONGITUD_X", "LATITUD_Y")

            # If district code is missing, we assign it using the district name dictionary:
            if "CODI_DISTRICTE" not in df.columns:
                df = df.withColumn("COD_DISTRICTE", district_map_expr.getItem(col("DES_DISTRICTE")))

            # If both codes are present, we cast them to integer and remove originals:
            if "CODI_BARRI" in df.columns:
                df = df.withColumn("COD_BARRI", col("CODI_BARRI").cast("int")) \
                       .withColumn("COD_DISTRICTE", col("CODI_DISTRICTE").cast("int")) \
                       .drop("CODI_BARRI", "CODI_DISTRICTE")

            # We extract the registration date from the expedition ID:
            df = df.withColumn("DAT_ALTA_EXPEDIENT", to_date(concat(lit("01-"), substring(col("ID_EXPEDIENT"), 1, 7)), "dd-MM-yyyy"))

            # We normalize the district names by removing accents:
            if "DES_DISTRICTE" in df.columns:
                df = df.withColumn("DES_DISTRICTE", unidecode_udf("DES_DISTRICTE")) \
                       .drop("DISTRICTE")

            # We save the cleaned DataFrame to the formatted zone:
            try:
                df.write.parquet(f"{formatted}/hut_{year}_{quarter}.parquet", mode='overwrite')
            except Exception as e:
                print(f"Failed to write parquet for year {year} and quarter {quarter}: {e}")

            # We store the cleaned DataFrame in a dictionary for future access:
            df_dict[df_name] = df
            print(f"Loaded: {df_name} from {file}")

        except Exception as e:
            print(f"Error loading {file}: {e}")

df_dict['hut_2019_3T'].show()

# First, we match files using the pattern "YYYY_pad_dom_mdbas_n":
pattern = re.compile(r"(20\d{2})_pad_dom_mdbas_n", re.IGNORECASE)

# We store each year's cleaned household dataset in a dictionary:
df_households = {}

# Then, again, we loop through files in the landing directory:
for file in os.listdir(landing):
    match = pattern.match(file)
    if match:
        year = match.group(1)
        df_name = f"pad_dom_{year}"
        full_path = os.path.join(landing, file)

        try:
            # We read the JSON file using multiline option to support nested structures:
            df = spark.read.option("multiline", True).json(full_path)

            # We rename the neighborhood column for consistency with other datasets:
            df = df.withColumnsRenamed({'Nom_Barri': 'DES_BARRI'})

            # We cast all relevant fields to appropriate data types and normalize the district name:
            df = df.withColumn("COD_AEB", col("AEB").cast("int")) \
                   .withColumn("DES_DISTRICTE", unidecode_udf('Nom_Districte')) \
                   .withColumn("COD_BARRI", col("Codi_Barri").cast("int")) \
                   .withColumn("COD_DISTRICTE", col("Codi_Districte").cast("int")) \
                   .withColumn("NUM_PERSONES_AGG", col("N_PERSONES_AGG").cast("int")) \
                   .withColumn("COD_SECCIO_CENSAL", col("Seccio_Censal").cast("int")) \
                   .withColumn("NUM_VALOR", col("Valor").cast("int")) \
                   .withColumn("DAT_REF", to_date(col("Data_Referencia"), "yyyy-MM-dd")) \
                   .drop(
                       'AEB', 'Codi_BARRI', 'Codi_DISTRICTE', 'Valor',
                       "Data_Referencia", "Seccio_Censal", 'N_PERSONES_AGG', 'Nom_Districte'
                   )

            # We write the cleaned file to the formatted zone in Parquet format:
            try:
                df.write.parquet(f"{formatted}/household_{year}_.parquet", mode='overwrite')
            except Exception as e:
                print(f"Failed to write parquet for year {year} : {e}")

            # We store the DataFrame in our dictionary:
            df_households[df_name] = df
            print(f"Loaded: {df_name} from {file}")
        except Exception as e:
            print(f"Error loading {file}: {e}")


df_households['pad_dom_2019'].show(5)

# This is how the Coworking data is with Sí and No:
df = spark.read.option("header", True).csv(f"{landing}/220930_censcomercialbcn_opendata_2022_v10_mod.csv")
df.select("SN_Coworking", "SN_Oci_Nocturn").distinct().show(20, truncate=False)


# We define a pattern to detect commercial census CSVs containing the year (e.g., "censcomercialbcn_2022_..."):
pattern = re.compile(r"^(?=.*censcomercialbcn.*)(?=.*(20\d{2})_).*$", re.IGNORECASE)

# We define a dictionary to store cleaned DataFrames by year:
df_comercial = {}

# We define a helper function to robustly parse "Sí"/"Si"/"sí"/"yes"/"1" etc. into binary indicator values:
normalize_udf = udf(lambda x: unidecode(x.lower().strip()) if x else "", StringType())
def parse_boolean_indicator(column):
    return when(normalize_udf(col(column)).isin("si", "yes", "true", "1"), 1).otherwise(0)

# We iterate through all matching files in the landing directory:
for file in os.listdir(landing):
    match = pattern.match(file)
    if match:
        year = match.group(1)
        df_name = f"comercial_{year}"
        full_path = os.path.join(landing, file)

        try:
            # We read the CSV file with headers:
            df = spark.read.option("header", True).csv(full_path)

            # We normalize the date column depending on the version:
            if "ID_Bcn_2019" in df.columns:
                df = df.withColumnRenamed("ID_Bcn_2019", "ID_Global") \
                       .withColumn("DAT_REV", to_date(col("Data_Revisio"), "yyyyMMdd")) \
                       .drop("Data_Revisio")
            else:
                df = df.withColumn("DAT_REV", to_date(col("Data_Revisio"), "yyyy-MM-dd")) \
                       .drop("Data_Revisio")

            # We standardize the naming of the cadastral reference column:
            if "Referencia_cadastral" in df.columns:
                df = df.withColumnRenamed("Referencia_cadastral", "ID_REFERENCIA_CATASTRAL")
            else:
                df = df.withColumnRenamed("Referencia_Cadastral", "ID_REFERENCIA_CATASTRAL")

            # We normalize activity codes depending on year-specific naming:
            if "Codi_Activitat_2019" in df.columns:
                df = df.withColumn("COD_ACTIVITAT_19", col("Codi_Activitat_2019").cast("int")) \
                       .drop("Codi_Activitat_2019")
            else:
                df = df.withColumn("COD_ACTIVITAT_22", col("Codi_Activitat_2022").cast("int")) \
                       .drop("Codi_Activitat_2022")

            # We rename all descriptive and location-based columns to follow a consistent naming convention:
            df = df.withColumnsRenamed({
                'ID_Global': 'ID_GLOBAL',
                'Nom_Principal_Activitat': 'DES_ACTIVITAT_PRINCIPAL',
                'Nom_Sector_Activitat': 'DES_SECTOR',
                'Nom_Grup_Activitat': 'DES_GRUP',
                'Nom_Activitat': 'DES_ACTIVITAT',
                'Nom_Local': 'DES_LOCAL',
                'Nom_Mercat': 'DES_MERCAT',
                'Nom_Galeria': 'DES_GALERIA',
                'Nom_CComercial': 'DES_CENTRE_COMERCIAL',
                'Nom_Eix': 'DES_EIX_COMERCIAL',
                'Direccio_Unica': 'DES_DIRECCIO_UNICA',
                'Nom_Via': 'DES_VIA',
                'Planta': 'COD_PLANTA',
                'Lletra_Inicial': 'COD_LLETRA_INICIAL',
                'Lletra_Final': 'COD_LLETRA_FINAL',
                'Nom_Barri': 'DES_BARRI',
                'Codi_Activitat_2016': 'COD_ACTIVITAT_16',
                'Porta': 'COD_PORTA'
            })

            # We cast and clean district, group, and activity identifiers to integer types:
            df = df.withColumn("COD_ACTIVITAT_PRINCIPAL", col("Codi_Principal_Activitat").cast("int")) \
                   .drop("Codi_Principal_Activitat") \
                   .withColumn("DES_DISTRICTE", unidecode_udf("Nom_Districte")) \
                   .drop("Nom_Districte") \
                   .withColumn("ID_2016", col("ID_Bcn_2016").cast("int")) \
                   .drop("ID_Bcn_2016") \
                   .withColumn("COD_SECTOR", col("Codi_Sector_Activitat").cast("int")) \
                   .drop("Codi_Sector_Activitat") \
                   .withColumn("COD_GRUP", col("Codi_Grup_Activitat").cast("int")) \
                   .drop("Codi_Grup_Activitat")

            # We apply the helper function to convert boolean indicator columns into clean binary flags:
            df = df.withColumn("IND_OCI_NOCTURN", parse_boolean_indicator("SN_Oci_Nocturn")).drop("SN_Oci_Nocturn") \
                   .withColumn("IND_COWORKING", parse_boolean_indicator("SN_Coworking")).drop("SN_Coworking") \
                   .withColumn("IND_SERVEI_DEGUSTACIO", parse_boolean_indicator("SN_Servei_Degustacio")).drop("SN_Servei_Degustacio") \
                   .withColumn("IND_OBERT24H", parse_boolean_indicator("SN_Obert24h")).drop("SN_Obert24h") \
                   .withColumn("IND_MIXT", parse_boolean_indicator("SN_Mixtura")).drop("SN_Mixtura") \
                   .withColumn("IND_PEU_CARRER", parse_boolean_indicator("SN_Carrer")).drop("SN_Carrer") \
                   .withColumn("IND_MERCAT", parse_boolean_indicator("SN_Mercat")).drop("SN_Mercat") \
                   .withColumn("IND_GALERIA", parse_boolean_indicator("SN_Galeria")).drop("SN_Galeria") \
                   .withColumn("IND_CENTRE_COMERCIAL", parse_boolean_indicator("SN_CComercial")).drop("SN_CComercial") \
                   .withColumn("IND_EIX_COMERCIAL", parse_boolean_indicator("SN_Eix")).drop("SN_Eix")

            # We cast all coordinate and numeric location fields to float or int for correct processing:
            df = df.withColumn("NUM_X_UTM_ETRS89", col("X_UTM_ETRS89").cast("float")).drop("X_UTM_ETRS89") \
                   .withColumn("NUM_Y_UTM_ETRS89", col("Y_UTM_ETRS89").cast("float")).drop("Y_UTM_ETRS89") \
                   .withColumn("NUM_LATITUD", col("Latitud").cast("float")).drop("Latitud") \
                   .withColumn("NUM_LONGITUD", col("Longitud").cast("float")).drop("Longitud") \
                   .withColumn("COD_VIA", col("Codi_Via").cast("int")).drop("Codi_Via") \
                   .withColumn("NUM_POLICIA_INICIAL", col("Num_Policia_Inicial").cast("int")).drop("Num_Policia_Inicial") \
                   .withColumn("NUM_POLICIA_FINAL", col("Num_Policia_Final").cast("int")).drop("Num_Policia_Final") \
                   .withColumn("COD_SOLAR", col("Solar").cast("int")).drop("Solar") \
                   .withColumn("COD_PARCELA", col("Codi_Parcela").cast("int")).drop("Codi_Parcela") \
                   .withColumn("COD_ILLA", col("Codi_Illa").cast("int")).drop("Codi_Illa") \
                   .withColumn("COD_SECCIO_CENSAL", col("Seccio_Censal").cast("int")).drop("Seccio_Censal") \
                   .withColumn("COD_BARRI", col("Codi_Barri").cast("int")).drop("Codi_Barri") \
                   .withColumn("COD_DISTRICTE", col("Codi_Districte").cast("int")).drop("Codi_Districte")

            # We store the cleaned DataFrame in our dictionary:
            df_comercial[df_name] = df

            # We write the cleaned and normalized data to the formatted zone, partitioned by district and neighborhood:
            df.write.partitionBy("COD_DISTRICTE", "COD_BARRI") \
                   .parquet(f"{formatted}/comercial_{year}.parquet", mode="overwrite")

            print(f"Loaded and saved: {df_name}")

        except Exception as e:
            print(f"Error processing file {file}: {e}")


# Confirm the changes that we had troubles with:
df = spark.read.parquet(f"{formatted}/comercial_2022.parquet")
df.groupBy("IND_COWORKING", "IND_OCI_NOCTURN").count().show()


# Final confirmation print
print("Data formatting pipeline executed successfully.")

# stop spark session if running standalone
spark.stop()