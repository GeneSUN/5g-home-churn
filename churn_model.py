
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

import sys
import subprocess
import traceback

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

if __name__ == "__main__":

    spark = SparkSession.builder.appName('Zhe_wifiscore_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()
    

    from pyspark.sql.types import StringType


    window_total_rows = Window.partitionBy("mdn_5g", "cust_id")
    window_spec = Window.partitionBy("mdn_5g", "cust_id").orderBy(F.rand())
    time_window = 30
    df_active_cpe = spark.read.parquet("/user/ZheS/5g_Churn/active_cpe_features")\
                        .withColumn( "churn_label", F.lit("N") )\
                        .withColumn('ACCT_NUM', F.col('ACCT_NUM').cast('int'))\
                        .filter( F.trim("ACTIVITY_CD")=="AC" )\
                        .withColumn("total_rows", F.count("*").over(window_total_rows))\
                        .withColumn("row_number", F.row_number().over(window_spec))\
                        .filter(F.col("row_number") <= time_window)\
                        .drop("row_number")
    df_churn_cpe = spark.read.parquet("/user/ZheS/5g_Churn/churn_cpe_features")\
                        .filter( F.trim("deact_cd")=="DE" )\
                        .filter( F.col("deact_dt")>"2024-06-15" )\
                        .filter( col("cpe_date")< col("deact_dt") )\
                        .filter( col("cpe_date")> col("deact_dt")-time_window )\
                        .withColumn("total_rows", F.count("*").over(window_total_rows))\
                        .withColumn( "churn_label", F.lit("Y") )\
                        .withColumn('ACCT_NUM', F.col('ACCT_NUM').cast('int'))

    cat_columns = ['mdn_5g', 'CUST_ID', 'CUST_LINE_SEQ_ID','PPLAN_CD', 'EQP_PROD_NM', 'COE_PPLAN_SUB_TYPE_DESC', 'MARKET_TYPE', 'ORDER_FULFILMENT', "churn_label"]
    
    score_columns = ['5g_uptime',
                     'scaled_sqrt_data_usage', 
                    'scaled_uploadresult', 'scaled_downloadresult', 'scaled_latency', 
                    'scaled_RSRP', 'scaled_avg_CQI', 'scaled_SNR', 
                    "scaled_switch_count_sum","scaled_reset_count","scaled_avg_MemoryPercentFree","scaled_percentageReceived",
                    'ACCT_NUM',
                    ]

    agg_expr = {col: "avg" for col in score_columns}

    df_union = df_active_cpe.select(cat_columns+score_columns)\
                            .union( df_churn_cpe.select(cat_columns+score_columns) )\
                            .groupBy(cat_columns)\
                            .agg( *[F.round(F.avg(col_name), 2).alias(f'{col_name}') for col_name in score_columns] )
                            
    keep_values = ['ARC-XCI55AX', 'WNC-CR200A', 'ASK-NCQ1338FA', 'FSNO21VA']

    onehot_columns = ['EQP_PROD_NM',  'COE_PPLAN_SUB_TYPE_DESC', 'MARKET_TYPE', 'ORDER_FULFILMENT']

    df_union = df_union.withColumn(
                                'COE_PPLAN_SUB_TYPE_DESC',
                                F.when(
                                    (F.col('COE_PPLAN_SUB_TYPE_DESC') == '5G Business Internet mmWave') | 
                                    (F.col('COE_PPLAN_SUB_TYPE_DESC') == '5G Business Internet C-Band'),
                                    '5G Business Internet'
                                ).otherwise(F.col('COE_PPLAN_SUB_TYPE_DESC'))
                            )\
                        .withColumn(
                            'EQP_PROD_NM',
                            F.element_at(F.split(F.col('EQP_PROD_NM'), ' '), -1)
                        )\
                        .withColumn(
                            'EQP_PROD_NM',
                            F.when(F.col('EQP_PROD_NM').isin(keep_values), F.col('EQP_PROD_NM')).otherwise("others")
                        )\
                        .fillna(value="unknown", subset=onehot_columns)
    from sklearn.preprocessing import OneHotEncoder
    


    df_pandas = df_union.toPandas()

    onehot_columns = ['EQP_PROD_NM', 'COE_PPLAN_SUB_TYPE_DESC', 'MARKET_TYPE', 'ORDER_FULFILMENT']
    encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')
    encoded_data = encoder.fit_transform(df_pandas[onehot_columns])
    encoded_df = pd.DataFrame(encoded_data, columns=encoder.get_feature_names_out(onehot_columns))
    df_final = pd.concat([df_pandas.drop(columns=onehot_columns), encoded_df], axis=1)

    selected_columns = score_columns + ['COE_PPLAN_SUB_TYPE_DESC_5G Business Internet', 'COE_PPLAN_SUB_TYPE_DESC_5G Home mmWave', 'ORDER_FULFILMENT_LOCALLY_FULFILLED', 'MARKET_TYPE_West', 'ORDER_FULFILMENT_SHIPPED', 'ORDER_FULFILMENT_unknown', 'MARKET_TYPE_unknown', 'COE_PPLAN_SUB_TYPE_DESC_5G Home C-Band', 'EQP_PROD_NM_others', 'MARKET_TYPE_Headquarters', 'EQP_PROD_NM_WNC-CR200A', 'MARKET_TYPE_South', 'EQP_PROD_NM_ASK-NCQ1338FA', 'COE_PPLAN_SUB_TYPE_DESC_4G LTE Home', 'EQP_PROD_NM_ARC-XCI55AX', 'churn_label', 'EQP_PROD_NM_FSNO21VA', 'MARKET_TYPE_East']
    df_selected = df_final[selected_columns]
    df_selected['churn_label'] = df_selected['churn_label'].map({'Y': 1, 'N': 0})
    df_selected['ACCT_NUM'] = pd.to_numeric(df_selected['ACCT_NUM'], errors='coerce').astype(int)

    #df_selected.fillna(df_selected.mean(), inplace=True)
    #df_selected.fillna(df_selected.median(), inplace=True)

    def fill_na_randomly(series):
        # Get non-null values from the series
        non_null_values = series.dropna().values
        # Replace NaN with a random choice from non-null values
        return series.apply(lambda x: np.random.choice(non_null_values) if pd.isna(x) else x)
    df_selected['5g_uptime'] = fill_na_randomly(df_selected['5g_uptime'])

    df_selected.fillna(df_selected.mean(), inplace=True)

    from sklearn.utils import resample

    df_majority = df_selected[df_selected.churn_label == 0]
    df_minority = df_selected[df_selected.churn_label == 1]

    df_majority_downsampled = resample(df_majority, 
                                    replace=False,    # Sample without replacement
                                    n_samples=len(df_minority),  # Match the minority class
                                    random_state=42)  # Set random seed for reproducibility

    df_downsampled = pd.concat([df_majority_downsampled, df_minority])

    from sklearn.model_selection import train_test_split

    X = df_downsampled.drop('churn_label', axis=1)
    y = df_downsampled['churn_label']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)


    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score

    rf_model = RandomForestClassifier(random_state=42)
    rf_model.fit(X_train, y_train)

    # Predictions and metrics for the training set
    y_train_pred = rf_model.predict(X_train)

    train_accuracy = accuracy_score(y_train, y_train_pred)
    print(f"Training Accuracy: {train_accuracy:.4f}")

    train_conf_matrix = confusion_matrix(y_train, y_train_pred)
    print("Training Confusion Matrix:")
    print(train_conf_matrix)

    train_class_report = classification_report(y_train, y_train_pred)
    print("Training Classification Report:")
    print(train_class_report)

    # Predictions and metrics for the test set
    y_test_pred = rf_model.predict(X_test)

    test_accuracy = accuracy_score(y_test, y_test_pred)
    print(f"Test Accuracy: {test_accuracy:.4f}")

    test_conf_matrix = confusion_matrix(y_test, y_test_pred)
    print("Test Confusion Matrix:")
    print(test_conf_matrix)

    test_class_report = classification_report(y_test, y_test_pred)
    print("Test Classification Report:")
    print(test_class_report)


    y_prob = rf_model.predict_proba(X_test)[:, 1]  # Probability for the positive class (churn)
    roc_auc = roc_auc_score(y_test, y_prob)
    print(f"ROC-AUC Score: {roc_auc:.4f}")

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt

    importances = rf_model.feature_importances_

    feature_names = X_train.columns  # or use the list of feature names if X_train is a numpy array

    feature_importance_df = pd.DataFrame({
        'Feature': feature_names,
        'Importance': importances
    })

    feature_importance_df = feature_importance_df.sort_values(by='Importance', ascending=False)

    print(feature_importance_df)


