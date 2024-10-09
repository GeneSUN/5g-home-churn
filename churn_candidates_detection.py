from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender
from pyspark.sql.functions import from_unixtime 
import argparse 
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *

from functools import reduce
from datetime import datetime, timedelta

def read_and_union_parquet_by_date(cpe_start_date_str: str, cpe_end_date_str: str, features=None):
    if features is None:
        features = ["dataScore","networkSpeedScore","networkSignalScore","networkFailureScore","deviceScore"]

    start_date = datetime.strptime(cpe_start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(cpe_end_date_str, '%Y-%m-%d')

    dfs = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        file_path = hdfs_pd + f"/user/ZheS/5g_homeScore/final_score/{date_str}"

        try:
            df = spark.read.parquet(file_path)\
                        .withColumn('cpe_date', F.lit(date_str))\
                        .select(['cpe_date',"mdn_5g","cust_id"] + features)
            dfs.append(df)
        except Exception as e:
            print(f"File {file_path} not found, skipping...")

        current_date += timedelta(days=1)

    if dfs:
        return reduce(DataFrame.union, dfs)
    else:
        print("No files found in the given date range.")
        return None

class CPEChurnAnalyzer:
    def __init__(self):
   
        self.churn_user_cpe_features = self.get_churn_features()
        self.active_not_churn_df = self.get_recent_active_not_churn_features()

    def get_churn_features(self):
        # load churn 
        df_churn_edw = spark.read.parquet(hdfs_pd + "/user/ZheS/5g_Churn/churn_df")\
                                    .withColumn("mdn_5g", F.trim(F.col("MTN")))
        
        earliest_churn_date  = df_churn_edw.agg(F.min("deact_dt")).collect()[0][0].strftime('%Y-%m-%d')
        end_date_str = df_churn_edw.agg(F.max("deact_dt")).collect()[0][0].strftime('%Y-%m-%d')

        # load latest cpe
        start_date_obj = datetime.strptime(earliest_churn_date, '%Y-%m-%d')
        cpe_start_date_str = (start_date_obj - timedelta(days=30)).strftime('%Y-%m-%d')

        self.cpe_all_records_df = read_and_union_parquet_by_date(cpe_start_date_str, end_date_str)
        latest_score_dates_df = self.cpe_all_records_df\
                                            .withColumn("cpe_date", F.to_date(F.col("cpe_date"), "yyyy-MM-dd"))\
                                            .groupBy("cust_id", "mdn_5g")\
                                            .agg(F.max("cpe_date").alias("latest_score_date"))
        
        churn_user_cpe = df_churn_edw.join(latest_score_dates_df.select("CUST_ID", "mdn_5g"), ["cust_id", "mdn_5g"])

        churn_user_cpe_features = churn_user_cpe.join( self.cpe_all_records_df, ["mdn_5g","cust_id"] )\
                                                .filter( col("cpe_date") <= col("deact_dt")  )\
                                                .filter( col("cpe_date") >= col("deact_dt") - 30  )

        return churn_user_cpe_features
    
    def get_recent_active_not_churn_features(self):
        
        fiveg_customer_df = spark.read.parquet( hdfs_pd + "/user/ZheS/5g_Churn/fiveg_customer_df")\
                            .withColumn("mdn_5g", F.trim(F.col("MTN")))\
                            .withColumn("ACTIVITY_CD", F.trim(F.col("ACTIVITY_CD")))

        filtered_df = fiveg_customer_df.filter(F.col("activity_cd").isin("AC", "D3", "DE"))

        # Filter to find records with only one "AC" and no "D3" or "DE"
        activity_count_df = filtered_df.groupBy("cust_id", "mdn_5g")\
                                        .agg(
                                            F.count(F.when(F.col("activity_cd") == "AC", 1)).alias("ac_count"),
                                            F.count(F.when(F.col("activity_cd").isin("D3", "DE"), 1)).alias("de_d3_count")
                                        )

        result_df = activity_count_df.filter(
                                                (F.col("ac_count") == 1) & (F.col("de_d3_count") == 0)
                                            )\
                                    .select("cust_id", "mdn_5g")


        df_recent_active = fiveg_customer_df.join(result_df, on=["cust_id", "mdn_5g"], how="inner")\
                                            .join(self.cpe_all_records_df, ["mdn_5g","cust_id"])

        return df_recent_active

if __name__ == "__main__":

    spark = SparkSession.builder.appName('5gchurn_features_ZheS')\
                        .config("spark.sql.adapative.enabled","true")\
                        .config("spark.ui.port","24045")\
                        .enableHiveSupport().getOrCreate()
    
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    analyzer = CPEChurnAnalyzer( )
    #print( analyzer.churn_user_cpe_features.columns )
    analyzer.churn_user_cpe_features.write.mode("overwrite").parquet(hdfs_pd + f"/user/ZheS/5g_Churn/churn_user_cpe_features" ) 
    analyzer.active_not_churn_df.write.mode("overwrite").parquet(hdfs_pd + f"/user/ZheS/5g_Churn/active_not_churn_df" ) 