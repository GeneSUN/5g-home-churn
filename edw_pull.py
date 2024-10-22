from pyspark.sql import SparkSession 
import argparse 
import requests 
import pandas as pd 
import json
from datetime import timedelta, date , datetime
import argparse 
import requests 
import pandas as pd 
import json
import psycopg2
import sys
import smtplib
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from ftplib import FTP
import pandas as pd

from hdfs import InsecureClient 
import os
import re 

def query_to_edw(temp_query, username='UNETNTLVMAS', hostserver='tddp.vzwcorp.com', password='Winter2024@'):
    import teradatasql
    # Initialize an empty list to store DataFrames
    df_list = []
    
    # Connect to the Teradata database
    with teradatasql.connect(host=hostserver, user=username, password=password, encryptdata="true") as connect:
        # Execute the query and read the result into a pandas DataFrame
        dfedw = pd.read_sql(temp_query, connect)
        df_list.append(dfedw)
    
    # Combine the DataFrames if needed and convert to Spark DataFrame
    spark_df = spark.createDataFrame(pd.concat(df_list))
    
    return spark_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName('edw pull')\
                        .config("spark.ui.port","24040")\
                        .getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")
    hdfs_pa  = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    hdfs_pd  = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'

    rpt_mth_value = 1230101

    churn_query = """
                WITH deactive_customer AS (
                    SELECT 
                        cust_id, 
                        cust_line_seq_id,
                        mtn, 
                        PPLAN_CD, 
                        EQP_PROD_NM,
                        CHANGE_REAS_CD,
                        acct_num, 
                        activity_dt AS deact_dt,
                        activity_cd AS deact_cd, 
                        coe_pplan_sub_type_desc
                    FROM dla_sum_fact_v
                    WHERE activity_cd IN ('de', 'd3') 
                    AND activity_dt >= {}
                    AND coe_pplan_sub_type_desc IN ('5G Business Internet mmWave', '5G Business Internet C-Band', '5G Home mmWave', '5G Home C-Band', '4G LTE Home')
                ),
                active_customer AS (
                    SELECT 
                        cust_id, 
                        mtn, 
                        activity_dt,
                        activity_cd
                    FROM dla_sum_fact_v
                    WHERE activity_cd IN ('ac', 're')
                    AND activity_dt >= {}
                    AND prepaid_ind = 'n'
                    AND rev_gen_ind = 'y'
                    AND managed_ind = 'c'
                    AND line_type_cd <> 't'
                ),
                Fixed_fiveg_features AS (
                    SELECT 
                        CUST_ID, 
                        mtn, 

                        case when CHNL_NM is null then null
                        when CHNL_NM = 'D2D' then 'D2D'
                        else 'Non-D2D'
                        end as chnl_nm,

                        install_type, 
                        MARKET_TYPE, 
                        ORDER_FULFILMENT, 
                        USAGE_FLAG
                    FROM NTL_PRD_ALLVM.FIXED_5G_SUMMARY_FACT_V
                )

                -- Perform the joins
                SELECT 
                    a.cust_id, 
                    cust_line_seq_id,
                    a.mtn, 
                    a.activity_dt, 
                    a.activity_cd,
                    d.PPLAN_CD, 
                    d.EQP_PROD_NM, 
                    d.CHANGE_REAS_CD, 
                    d.acct_num, 
                    d.deact_dt, 
                    d.deact_cd, 
                    d.coe_pplan_sub_type_desc,
                    f.chnl_nm, 
                    f.install_type, 
                    f.MARKET_TYPE, 
                    f.ORDER_FULFILMENT, 
                    f.USAGE_FLAG
                FROM active_customer a
                INNER JOIN deactive_customer d
                    ON a.cust_id = d.cust_id 
                    AND a.mtn = d.mtn
                LEFT JOIN Fixed_fiveg_features f
                    ON a.cust_id = f.cust_id
                    AND a.mtn = f.mtn;
    """.format(rpt_mth_value,rpt_mth_value)
    

    fiveg_customer_query = """
            WITH Fixed_fiveg AS (
                SELECT
                    CUST_ID,
                    mtn,

                    case when CHNL_NM is null then null
                    when CHNL_NM = 'D2D' then 'D2D'
                    else 'Non-D2D'
                    end as chnl_nm,
                    
                    install_type,
                    MARKET_TYPE,
                    ORDER_FULFILMENT,
                    USAGE_FLAG
                FROM NTL_PRD_ALLVM.FIXED_5G_SUMMARY_FACT_V
            )

            SELECT 
                dsf.cust_id,
                dsf.cust_line_seq_id,
                dsf.mtn,
                dsf.activity_dt,
                dsf.activity_cd,
                dsf.PPLAN_CD,
                dsf.eqp_prod_nm,
                dsf.CHANGE_REAS_CD,
                dsf.acct_num,
                --dsf.rpt_mth,
                --dsf.prepaid_ind,
                --dsf.rev_gen_ind,
                --dsf.managed_ind,
                --dsf.line_type_cd,
                dsf.coe_pplan_sub_type_desc,
                ffg.chnl_nm,
                ffg.install_type,
                ffg.MARKET_TYPE,
                ffg.ORDER_FULFILMENT,
                ffg.USAGE_FLAG
            FROM dla_sum_fact_v dsf
            LEFT JOIN Fixed_fiveg ffg
                ON dsf.cust_id = ffg.cust_id
                AND dsf.mtn = ffg.mtn
            WHERE (dsf.rpt_mth >= {} OR dsf.activity_dt >= {})
            AND dsf.coe_pplan_sub_type_desc IN ('5G Business Internet mmWave', '5G Business Internet C-Band', '5G Home mmWave', '5G Home C-Band', '4G LTE Home')
            """.format(rpt_mth_value, rpt_mth_value)

    """    """
    fiveg_customer_df = query_to_edw(fiveg_customer_query)
    fiveg_customer_df.write\
                    .mode("overwrite")\
                    .parquet(hdfs_pd + f"/user/ZheS/5g_Churn/fiveg_customer_df" )



    churn_df = query_to_edw(churn_query)

    hdfs_pa  = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    hdfs_pd  = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    churn_df.write\
            .mode("overwrite")\
            .parquet(hdfs_pd + f"/user/ZheS/5g_Churn/churn_df" )   



    sys.exit()
