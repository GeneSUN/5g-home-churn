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

def read_and_union_parquet_by_date(start_date, end_date, base_path, features = ["dataScore","networkSpeedScore","networkSignalScore","networkFailureScore","deviceScore","5gHomeScore"]):

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    dfs = []

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        file_path = f"{base_path}/{date_str}"

        try:
            
            df = spark.read.parquet(file_path)\
                        .withColumn('date', lit(date_str))\
                        .select(['date',"sn","imei","imsi","mdn_5g","cust_id"]+features)
        
            dfs.append(df)
        except Exception as e:
            print(f"File {file_path} not found, skipping...")

        current_date += timedelta(days=1)


    if dfs:
        result_df = dfs[0]
        for df in dfs[1:]:
            result_df = result_df.union(df)
            
        #reference_date = to_date(lit(date_str), "yyyy-MM-dd")  
        #result_df = result_df.withColumn("max_churn_days", datediff(reference_date, to_date(col("date"), "yyyy-MM-dd")))
        return result_df
    else:
        print("No files found in the given date range.")
        return None



if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder\
            .appName('5gHome_crsp')\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    base_path = hdfs_pd + "/user/ZheS/5g_homeScore/final_score"
    # step 1
    latest_score_dates_df = read_and_union_parquet_by_date("2024-04-21", "2024-08-29", base_path)\
                                .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))\
                                .groupBy("cust_id", "mdn_5g")\
                                .agg( F.max("date").alias("latest_score_date") )

    # step 2
    df_churn_edw = spark.read.parquet(hdfs_pd + "/user/ZheS/5g_Churn/churn_df")\
                        .withColumn( "mdn_5g", F.trim( F.col("MTN") ) )

    # step 3
    start_date_str = '2024-06-01'; start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date_str = '2024-7-31'; end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') 
                for i in range((end_date - start_date).days + 1)]
    matches_df_list = []
    results = []
    from pyspark.sql import Row

    for deact_date in date_list:

        matches_with_date = df_churn_edw.select("CUST_ID","mdn_5g", "deact_dt")\
                                        .filter(col("deact_dt") == deact_date)\
                                        .join(  latest_score_dates_df.select("CUST_ID","mdn_5g"), ["cust_id", "mdn_5g"]  )

        matches_df_list.append(matches_with_date)

    cpe_churn_candidates = reduce(DataFrame.union, matches_df_list)
    cpe_churn_candidates.repartition(10)\
                        .write\
                        .mode("overwrite")\
                        .parquet(hdfs_pd + f"//user/ZheS/5g_Churn/cpe_churn_matches_candidates")
    
    # add cpe information
    cpe_all_records = read_and_union_parquet_by_date("2024-05-21", "2024-06-29", base_path)

    cpe_churn_candidates.join(cpe_all_records, ["mdn_5g","cust_id"])\
                        .filter( col("deact_dt")>col("date"))
    
    # add edw information

    """
    for deact_date in date_list:

        total_cpe_ceased = latest_score_dates_df.filter( col("latest_score_date")==deact_date ).count()
        total_churn = df_churn_edw.filter(col("deact_dt")==deact_date).count()
        total_matches = df_churn_edw.filter(F.col("deact_dt") == deact_date)\
                                    .join(latest_score_dates_df, ["cust_id","mdn_5g"])\
                                    .count()
        percentage_of_total_matches = (total_matches / total_churn) if total_churn > 0 else 0

        print(f"\nCounts of churn customers by the latest date they had a CPE score before {deact_date}:")
        df_churn_edw.filter(col("deact_dt") == deact_date)\
                    .join( latest_score_dates_df, ["cust_id","mdn_5g"] )\
                    .groupby("latest_score_date")\
                    .count()\
                    .orderBy(F.desc("latest_score_date"))\
                    .show()

        results.append(Row(
                            deact_date=deact_date,
                            total_cpe_ceased=total_cpe_ceased,
                            total_churn=total_churn,
                            total_matches=total_matches,
                            percentage_of_total_matches=percentage_of_total_matches
                        ))

    results_df = spark.createDataFrame(results)
    results_df.coalesce(1)\
            .write\
            .csv(hdfs_pd + f"//user/ZheS/5g_Churn/cpe_churn_matches", header=True, mode='overwrite')
    """