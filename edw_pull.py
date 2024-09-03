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

def query_to_edw(temp_query, username='UNETNTLVMAS', hostserver='tddp.vzwcorp.com', password='Fall2024@'):
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

    churn_query = """
                SELECT
                segment
                ,price_plan_cd
                ,activity_cd
                ,eqp_prod_nm
                ,technology
                ,act_mth
                ,act_dt
                ,deact_dt
                ,deact_mth
                ,dayson_dt-act_dt as days_on
                ,DISCO_REASON
                ,ACTIVITY_DESC
                ,CHANGE_REAS_DESC
                ,CHNL_NM
                ,install_type
                ,sales_dt
                ,install_dt
                ,LEFT(x.mtn, 10) mtn
                ,x.cust_id
                FROM(
                SELECT
                x.segment
                ,price_plan_cd
                ,x.activity_cd
                ,x.eqp_prod_nm
                ,technology
                ,act_dt-extract(day from act_dt)+1 as act_mth
                ,act_dt
                ,deact_dt
                ,deact_dt-extract(day from deact_dt)+1 as deact_mth
                ,case when deact_dt is null then current_date else deact_dt end as dayson_dt
                ,CASE when b.CHANGE_REAS_DESC like '%MOVING%' THEN 'Movers'
                WHEN b.CHANGE_REAS_CTGRY_CD = 'I' THEN 'Involuntary'
                WHEN b.CHANGE_REAS_CTGRY_CD = 'A' THEN 'Involuntary'
                when B.CHANGE_REAS_DESC  is null then null
                ELSE 'Voluntary'  END AS DISCO_REASON
                ,d.ACTIVITY_DESC
                ,b.CHANGE_REAS_DESC
                ,x.cust_id
                ,x.mtn
                ,case when C.CHNL_NM is null then null
                when C.CHNL_NM = 'D2D' then 'D2D'
                else 'Non-D2D'
                end as CHNL_NM
                ,c.install_type
                ,c.sales_dt
                ,c.install_dt
                FROM
                (
                SELECT
                coalesce(sc2.vz2_segmt_ctgry_desc, 'Wireless Consumer Revenue') as segment
                ,dla.cust_id
                ,dla.mtn
                ,dla.PPLAN_CD AS price_plan_cd
                ,CASE WHEN dla.PPLAN_CD IN ('67571','67576','50127','50129','50128','50130','53617','51219') THEN 'C-Band'
                WHEN dla.PPLAN_CD IN ('67567','67568','50044','50116','50055','50117','17542','25878','32525','32523','39425','39428','32780','32781') THEN 'MM Wave'
                WHEN P.coe_pplan_type_desc = 'Fixed Wireless Home' THEN P.COE_PPLAN_SUB_TYPE_DESC
                ELSE 'Other'
                END AS technology
                ,de.activity_dt as deact_dt
                ,de.activity_cd
                ,de.eqp_prod_nm
                ,de.CHANGE_REAS_CD
                ,dla.acct_num
                ,min(dla.activity_dt) as act_dt
                FROM dla_sum_fact_v dla
                LEFT JOIN NTL_PRD_ALLVM.PRICE_PLAN_V P
                ON P.PPLAN_CD = dla.PPLAN_CD
                LEFT OUTER JOIN coe_prd_allvm.cust_acct_line_5g_xref_dly_hist_v f 
                ON dla.cust_id = f.cust_id 
                AND dla.cust_line_seq_id = f.cust_line_seq_id 
                AND f.active_ind = 'y' 
                AND dla.activity_dt+1 between f.eff_dt and f.exp_dt 
                AND f.network_type = '5g fixed wireless'
                LEFT OUTER JOIN cust_acct_line_dim_dly_hist_v sc --curr
                ON dla.cust_id = sc.cust_id
                AND dla.cust_line_seq_id = sc.cust_line_seq_id
                AND add_months(dla.activity_dt-extract(day from dla.activity_dt)+1,1)-1 between sc.eff_dt and sc.exp_dt
                AND sc.dim_name = 'vz2_segmt'
                LEFT OUTER JOIN ntl_prd_allvm.vz2_segmt_dim_ref_v sc2 --curr
                ON sc.dim_value = sc2.vz2_segmt_cd
                AND sc2.curr_prev_ind = 'c'
                LEFT JOIN dla_sum_fact_v de
                
                --INNER JOIN dla_sum_fact_v de --
                
                ON dla.cust_id = de.cust_id
                AND dla.cust_line_seq_id = de.cust_line_seq_id
                AND de.activity_cd in ('de','d3') 
                AND de.activity_dt > dla.activity_dt 
                
                AND de.rpt_mth >= 1240101
                WHERE dla.rpt_mth >= 1240101
                --AND de.rpt_mth >= 1190101--
                --WHERE dla.rpt_mth >= 1190101--
                
                AND dla.prepaid_ind = 'n'
                AND dla.rev_gen_ind = 'y'
                AND dla.managed_ind = 'c'
                AND dla.line_type_cd <> 't'
                AND dla.activity_cd in ('ac','re')
                AND segment = 'Wireless Consumer Revenue'
                AND (f.cust_id is not null or (dla.coe_pplan_sub_type_desc in('5G Business Internet','5G Home', '4g lte home')))
                GROUP BY 1,2,3,4,5,6,7,8,9,10) x
                --at fwa line activation--
                INNER JOIN ntl_prd_allvm.CUST_ACCT_DLY_HIST_V h
                ON h.CUST_ID = x.CUST_ID
                AND h.ACCT_NUM = x.ACCT_NUM
                AND x.act_dt between h.eff_dt and h.exp_dt
                LEFT JOIN NTL_PRD_ALLVM.CHANGE_REASON_V B
                ON x.CHANGE_REAS_CD = B.CHANGE_REAS_CD
                LEFT JOIN NTL_PRD_ALLVM.ACTIVITY_V d
                ON x.ACTIVITY_CD = d.ACTIVITY_CD
                LEFT JOIN (select
                CUST_ID
                ,mtn
                ,chnl_nm
                ,install_type
                ,sales_dt
                ,install_dt
                FROM NTL_PRD_ALLVM.FIXED_5G_SUMMARY_FACT_V
                GROUP BY 1,2,3,4,5,6)C
                ON c.CUST_ID = x.CUST_ID
                AND c.mtn = x.mtn
                qualify rank() OVER (PARTITION BY x.cust_id,x.mtn ORDER BY deact_dt desc) = 1) x
                WHERE technology <> 'other'
            """
    all_customer_query = """
                    SELECT cust_id,
                            cust_line_seq_id,
                            mtn,
                            PPLAN_CD,
                            activity_dt,
                            activity_cd,  
                            eqp_prod_nm, 
                            acct_num, 
                            CHANGE_REAS_CD, 
                            rpt_mth, 
                            prepaid_ind, 
                            rev_gen_ind, 
                            managed_ind, 
                            line_type_cd, 
                            coe_pplan_sub_type_desc
                    FROM dla_sum_fact_v
                    WHERE rpt_mth >= 1240101
                """
    import teradatasql

    churn_df = query_to_edw(churn_query)

    hdfs_pa  = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    hdfs_pd  = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    churn_df.write\
            .mode("overwrite")\
            .parquet(hdfs_pd + f"/user/ZheS/5g_Churn/df_churn" )