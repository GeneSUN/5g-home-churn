from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender
from pyspark.sql.functions import from_unixtime 
import argparse 
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from functools import reduce
from datetime import datetime, timedelta

class CPEChurnAnalyzer:
    def __init__(self, cpe_path: str, churn_path: str):

        self.cpe_path = cpe_path
        self.churn_path = churn_path
        self.latest_score_dates_df = None
        self.df_churn_edw = None
        self.cpe_all_records = None
        self.cpe_churn_candidates = None
        self.active_candidates = None

    def read_and_union_parquet_by_date(self, start_date_str: str, end_date_str: str, features=None):
        if features is None:
            features = ["dataScore","networkSpeedScore","networkSignalScore","networkFailureScore","deviceScore"]

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        dfs = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            file_path = f"{self.cpe_path}/{date_str}"

            try:
                df = spark.read.parquet(file_path)\
                            .withColumn('date', F.lit(date_str))\
                            .select(['date',"mdn_5g","cust_id"] + features)
                dfs.append(df)
            except Exception as e:
                print(f"File {file_path} not found, skipping...")

            current_date += timedelta(days=1)

        if dfs:
            return reduce(DataFrame.union, dfs)
        else:
            print("No files found in the given date range.")
            return None

    def filter_recent_active_customers(self, df):
        
        filtered_df = df.filter(F.col("activity_cd").isin("AC", "D3", "DE"))

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

        # Find all records in the original DataFrame whose "cust_id" and "mdn_5g" are in result_df
        final_df = df.join(result_df, on=["cust_id", "mdn_5g"], how="inner")

        return final_df

    def load_data(self, start_date_str = "2024-04-21", end_date_str =  "2024-08-29"):

        self.latest_score_dates_df = self.read_and_union_parquet_by_date(start_date_str, end_date_str)\
                                            .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))\
                                            .groupBy("cust_id", "mdn_5g")\
                                            .agg(F.max("date").alias("latest_score_date"))

        self.df_churn_edw = spark.read.parquet(self.churn_path)\
                                    .withColumn("mdn_5g", F.trim(F.col("MTN")))

        self.cpe_all_records = self.read_and_union_parquet_by_date(start_date_str, end_date_str)\

    def analyze_churn(self, start_date_str: str, end_date_str: str, df_churn_edw = None, latest_score_dates_df = None):

        if df_churn_edw is None:
            df_churn_edw = self.df_churn_edw
        if latest_score_dates_df is None:
            latest_score_dates_df = self.latest_score_dates_df

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d')
                        for i in range((end_date - start_date).days + 1)]

        matches_df_list = []
        recent_active_list = []

        fiveg_customer_df = spark.read.parquet( hdfs_pd + "/user/ZheS/5g_Churn/fiveg_customer_df")\
                            .withColumn("mdn_5g", F.trim(F.col("MTN")))\
                            .withColumn("ACTIVITY_CD", F.trim(F.col("ACTIVITY_CD")))
        df_recent_active = self.filter_recent_active_customers( fiveg_customer_df )

        for deact_date in date_list:
            matches_with_date = df_churn_edw.select("CUST_ID", "mdn_5g", "deact_dt")\
                                            .filter(F.col("deact_dt") == deact_date)\
                                            .join(latest_score_dates_df.select("CUST_ID", "mdn_5g"), ["cust_id", "mdn_5g"])

            matches_df_list.append( matches_with_date )
            recent_active_list.append( df_recent_active.filter(F.col("ACTIVITY_DT") == deact_date) )

        if matches_df_list:
            self.cpe_churn_candidates = reduce(DataFrame.union, matches_df_list)
            self.active_candidates = reduce(DataFrame.union, recent_active_list)

        else:
            print("No matches found.")


    def add_features_churn(self, cpe_churn_candidates = None, cpe_all_records = None, df_churn_edw = None):
        if cpe_churn_candidates is None:
            cpe_churn_candidates = self.cpe_churn_candidates
        if cpe_all_records is None:
            cpe_all_records = self.cpe_all_records
        if df_churn_edw is None:
            df_churn_edw = self.df_churn_edw        

        # add cpe_features
        cpe_churn_candidates.join(cpe_all_records, ["mdn_5g","cust_id"])\
                            .filter( col("deact_dt")>=col("date"))\
                            .join(df_churn_edw, ["mdn_5g","cust_id"] )

if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder.appName('5gHome_crsp')\
                        .config("spark.sql.adapative.enabled","true")\
                        .enableHiveSupport().getOrCreate()
    
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    analyzer = CPEChurnAnalyzer( cpe_path = hdfs_pd + "/user/ZheS/5g_homeScore/final_score", 
                                 churn_path = hdfs_pd + "/user/ZheS/5g_Churn/churn_df")
    analyzer.load_data()
    analyzer.analyze_churn("2024-06-01", "2024-06-11")

    analyzer.cpe_churn_candidates.show()
    analyzer.active_candidates.show()
    
    """
    if cpe_churn_candidates:
        cpe_churn_candidates.coalesce(1).write.csv("hdfs://path_to_output.csv", header=True)
    """