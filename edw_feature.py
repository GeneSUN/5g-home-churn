from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd 
import subprocess
import traceback
import teradatasql

def query_to_edw(temp_query, username='UNETNTLVMAS', hostserver='tddp.vzwcorp.com', password='Winter2024@'):

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

    spark = SparkSession.builder.appName('Zhe_wifiscore_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    rpt_mth_value = 1230101
    trail_query = """
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
            WHERE (rpt_mth >= {} OR activity_dt >= {})
            AND coe_pplan_sub_type_desc IN ('5G Business Internet mmWave','5G Business Internet C-Band','5G Home mmWave','5G Home C-Band','4G LTE Home')
        """.format(rpt_mth_value, rpt_mth_value)
    
    install_query = """
            WITH Fixed_fiveg AS (
                SELECT
                    CUST_ID,
                    mtn,
                    chnl_nm,
                    install_type,
                    sales_dt,
                    install_dt,
                    DWELLING_TYPE,
                    CUST_TYPE_CD,
                    MARKET_TYPE,
                    CMA_NM,
                    CUST_EMAIL_ADDR,
                    SVC_STATE_CD,
                    CUST_TYPE_DESC,
                    VERTICAL,
                    STATUS,
                    ORDER_FULFILMENT,
                    SKU_ID,
                    DEVICE_TYPE_DESC,
                    PPLAN_CTGRY_DESC,
                    USAGE_FLAG,
                    PRE_CHECK_STATUS,
                    DISCOUNT_SVC_PROD_DESC
                FROM NTL_PRD_ALLVM.FIXED_5G_SUMMARY_FACT_V
            )

            SELECT 
                dsf.cust_id,
                dsf.cust_line_seq_id,
                dsf.mtn,
                dsf.PPLAN_CD,
                dsf.activity_dt,
                dsf.activity_cd,
                dsf.eqp_prod_nm,
                dsf.acct_num,
                dsf.CHANGE_REAS_CD,
                dsf.rpt_mth,
                dsf.prepaid_ind,
                dsf.rev_gen_ind,
                dsf.managed_ind,
                dsf.line_type_cd,
                dsf.coe_pplan_sub_type_desc,
                ffg.chnl_nm,
                ffg.install_type,
                ffg.sales_dt,
                ffg.install_dt,
                ffg.DWELLING_TYPE,
                ffg.CUST_TYPE_CD,
                ffg.MARKET_TYPE,
                ffg.CMA_NM,
                ffg.CUST_EMAIL_ADDR,
                ffg.SVC_STATE_CD,
                ffg.CUST_TYPE_DESC,
                ffg.VERTICAL,
                ffg.STATUS,
                ffg.ORDER_FULFILMENT,
                ffg.SKU_ID,
                ffg.DEVICE_TYPE_DESC,
                ffg.PPLAN_CTGRY_DESC,
                ffg.USAGE_FLAG,
                ffg.PRE_CHECK_STATUS,
                ffg.DISCOUNT_SVC_PROD_DESC
            FROM dla_sum_fact_v dsf
            LEFT JOIN Fixed_fiveg ffg
                ON dsf.cust_id = ffg.cust_id
                AND dsf.mtn = ffg.mtn
            WHERE (dsf.rpt_mth >= {} OR dsf.activity_dt >= {})
            AND dsf.coe_pplan_sub_type_desc IN ('5G Business Internet mmWave', '5G Business Internet C-Band', '5G Home mmWave', '5G Home C-Band', '4G LTE Home')
            """.format(rpt_mth_value, rpt_mth_value)

    import teradatasql

    fiveg_customer_df = query_to_edw(install_query)
    fiveg_customer_df.write\
                    .mode("overwrite")\
                    .parquet(hdfs_pd + f"/user/ZheS/5g_Churn/explore_features/install_query_df" )