import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os

# Créer les répertoires si nécessaire
os.makedirs("structured_table", exist_ok=True)

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("TP_DATA_INTEGRATION") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Schéma pour le fichier LTM_Data_2022_8_1
LTM_Data_2022_8_1 =  StructType([
    StructField("SITE_ID", StringType(), True),
    StructField("PROGRAM_ID", StringType(), True),
    StructField("DATE_SMP", DateType(), True),
    StructField("SAMPLE_LOCATION", StringType(), True),
    StructField("SAMPLE_TYPE", StringType(), True),
    StructField("WATERBODY_TYPE", StringType(), True),
    StructField("SAMPLE_DEPTH", FloatType(), True),
    StructField("TIME_SMP", StringType(), True),
    StructField("ANC_UEQ_L", FloatType(), True),
    StructField("CA_UEQ_L", FloatType(), True),
    StructField("CHL_A_UG_L", FloatType(), True),
    StructField("CL_UEQ_L", FloatType(), True),
    StructField("COND_UM_CM", FloatType(), True),
    StructField("DOC_MG_L", FloatType(), True),
    StructField("F_UEQ_L", FloatType(), True),
    StructField("K_UEQ_L", FloatType(), True),
    StructField("MG_UEQ_L", FloatType(), True),
    StructField("NA_UEQ_L", FloatType(), True),
    StructField("NH4_UEQ_L", FloatType(), True),
    StructField("NO3_UEQ_L", FloatType(), True),
    StructField("N_TD_UEQ_L", FloatType(), True),
    StructField("PH_EQ", FloatType(), True),
    StructField("PH_FLD", FloatType(), True),
    StructField("PH_LAB", FloatType(), True),
    StructField("PH_STVL", FloatType(), True),
    StructField("P_TL_UEQ_L", FloatType(), True),
    StructField("SECCHI_M", FloatType(), True),
    StructField("SIO2_MG_L", FloatType(), True),
    StructField("SO4_UEQ_L", FloatType(), True),
    StructField("WTEMP_DEG_C", FloatType(), True)
])

# Schéma pour le fichier Methods_2022_8_1
Methods_2022_8_1 = StructType([
    StructField("PROGRAM_ID", StringType(), True),
    StructField("PARAMETER", StringType(), True),
    StructField("START_YEAR", FloatType(), True),
    StructField("END_YEAR", FloatType(), True),
    StructField("METHOD", StringType(), True),
    StructField("METHOD_DESCRIPTION", StringType(), True)
])

# Schéma pour le fichierSite_Information_2022_8_1
Site_Information_2022_8_1 = StructType([
    StructField("SITE_ID", FloatType(), True),
    StructField("PROGRAM_ID", StringType(), True),
    StructField("LATDD", FloatType(), True),
    StructField("LONDD", FloatType(), True),
    StructField("LATDD_CENTROID", FloatType(), True),
    StructField("LONDD_CENTROID", FloatType(), True),
    StructField("SITE_NAME", StringType(), True),
    StructField("COMID", FloatType(), True),
    StructField("FEATUREID", FloatType(), True),
    StructField("COUNTY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("ECOREGION_I", StringType(), True),
    StructField("ECOREGION_II", StringType(), True),
    StructField("ECOREGION_III", StringType(), True),
    StructField("ECOREGION_IV", StringType(), True),
    StructField("ECONAME_I", StringType(), True),
    StructField("ECONAME_II", StringType(), True),
    StructField("ECONAME_III", StringType(), True),
    StructField("lECONAME_IV", StringType(), True),
    StructField("LAKE_DEPTH_MAX", FloatType(), True),
    StructField("LAKE_DEPTH_MEAN", FloatType(), True),
    StructField("LAKE_AREA_HA", FloatType(), True),
    StructField("LAKE_AREA_NHD2", FloatType(), True),
    StructField("WSHD_AREA_HA", FloatType(), True),
    StructField("SITE_ELEV", FloatType(), True),
    StructField("WSHD_ELEV_AVG", FloatType(), True),
    StructField("WSHD_ELEV_MIN", FloatType(), True),
    StructField("WSHD_ELEV_MAX", FloatType(), True),
    StructField("WSHD_SLOPE_AVG", FloatType(), True),
    StructField("UPDATE_DATE", DateType(), True)
])

# Lire le fichier Excel avec Pandas
ltm_pandas_df = pd.read_excel("LTM_Data_2022_8_1.xlsx")
methods_pandas_df = pd.read_excel("Methods_2022_8_1.xlsx")
site_info_pandas_df = pd.read_excel("Site_Information_2022_8_1.xlsx")

# Convertir les DataFrames Pandas en DataFrames Spark
ltm_spark_df = spark.createDataFrame(ltm_pandas_df, schema=LTM_Data_2022_8_1)
methods_spark_df = spark.createDataFrame(methods_pandas_df, )
site_info_spark_df = spark.createDataFrame(site_info_pandas_df)

# Moyenne annuelle de PH_EQ par site
ph_avg = ltm_spark_df.groupBy("SITE_ID", F.year("DATE_SMP").alias("YEAR")).agg(F.avg("PH_EQ").alias("PH_AVG_ANNUAL"))

# Variance des concentrations de SO4_UEQ_L par région
cl_ueq_l_variance = ltm_spark_df.join(site_info_spark_df, "SITE_ID").groupBy("STATE").agg(F.variance("CL_UEQ_L").alias("CL_UEQ_L_VARIANCE"))

# Température moyenne de l'eau par type de cours d'eau
temp_avg = ltm_spark_df.groupBy("WATERBODY_TYPE").agg(F.avg("WTEMP_DEG_C").alias("TEMP_AVG"))

ph_avg.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("structured_table/ph_avg") 
cl_ueq_l_variance.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("structured_table/cl_ueq_l_variance")
temp_avg.write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("structured_table/temp_avg")