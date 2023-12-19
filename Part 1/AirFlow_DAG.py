import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

########################################################
#
#   DAG Settings
#
#########################################################

# set up pg connection
ps_pg_hook = PostgresHook(postgres_conn_id="postgres_default")
conn_ps = ps_pg_hook.get_conn()


dag_default_args = {
    'owner': 'Kiran Das',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}
# set up dag
dag = DAG(
    dag_id='AListings',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
BDE = AIRFLOW_DATA+"/BDE_AT3/"
listings = BDE +"listings"
census = BDE+"Census_LGA"
LGA= BDE+"NSW_LGA"

def import_listings(**kwargs):

    # Get a list of all CSV files in the directory
    filelist = [k for k in os.listdir(listings) if '.csv' in k]

    if not filelist:
        print("No CSV files found in the directory.")
        return


    for file_name in filelist:
        file_path = os.path.join(listings, file_name)

        # Generate a dataframe from the current CSV file
        df = pd.read_csv(file_path)

        # Cast numeric columns to int or float
        numeric_columns = ['LISTING_ID', 'SCRAPE_ID', 'ACCOMMODATES', 'AVAILABILITY_30', 'NUMBER_OF_REVIEWS']

        for col in numeric_columns:
            try:
                df[col] = df[col].astype(int)
            except ValueError as e:
                print(f"Error converting column '{col}' to int: {str(e)}")

        float_columns = [
            'PRICE', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY',
            'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN',
            'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_VALUE'
        ]

        for col in float_columns:
            df[col] = df[col].astype(float)

        if not df.empty:
            col_names = list(df.columns)  # Use all columns
            insert_sql = f"""
                INSERT INTO raw.listings ({', '.join(col_names)})
                VALUES %s
            """

            values = [list(row) for row in df.itertuples(index=False)]

            execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
            print(f"Inserted {len(df)} records from '{file_name}' into the database.")
            conn_ps.commit()
        else:
            print(f"No records to insert from '{file_name}'.")

    return None


def import_2016Census_G01_NSW_LGA(**kwargs):
    file_name= '2016Census_G01_NSW_LGA.csv'
    file_path = os.path.join(census, file_name)
    df = pd.read_csv(file_path)
    if not df.empty:
        col_names = list(df.columns)  # Use all columns
        insert_sql = f"""
        INSERT INTO "raw"."2016Census_G01_NSW_LGA" ({', '.join(col_names)})
                                VALUES %s
                                """
                    #values = [tuple(row) for row in df[col_names].to_records(index=False)]
        values = [list(row) for row in df[col_names].itertuples(index=False)] 

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        print(f"Inserted {len(df)} records into the database.")
        conn_ps.commit()

    else:
        print("No records to insert.")
# Create a PythonOperator to run the import_load_facts_func

def import_2016Census_G02_NSW_LGA(**kwargs):
    file_name= '2016Census_G02_NSW_LGA.csv'
    file_path = os.path.join(census, file_name)
    df = pd.read_csv(file_path)
    if not df.empty:
        col_names = list(df.columns)  # Use all columns
        insert_sql = f"""
        INSERT INTO "raw"."2016Census_G02_NSW_LGA" ({', '.join(col_names)})
                                VALUES %s
                                """
                    #values = [tuple(row) for row in df[col_names].to_records(index=False)]
        values = [list(row) for row in df[col_names].itertuples(index=False)] 

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        print(f"Inserted {len(df)} records into the database.")
        conn_ps.commit()

    else:
        print("No records to insert.")
# Create a PythonOperator to run the import_nsw_lga_func
def import_NSW_LGA_CODE(**kwargs):
    file_name= 'NSW_LGA_CODE.csv'
    file_path = os.path.join(LGA, file_name)
    df = pd.read_csv(file_path)
    if not df.empty:
        col_names = list(df.columns)  # Use all columns
        insert_sql = f"""
        INSERT INTO raw.NSW_LGA_CODE ({', '.join(col_names)})
                                VALUES %s
                                """
     #values = [tuple(row) for row in df[col_names].to_records(index=False)]
        values = [list(row) for row in df[col_names].itertuples(index=False)] 

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        print(f"Inserted {len(df)} records into the database.")
        conn_ps.commit()

    else:
        print("No records to insert.")
# Create a PythonOperator to run the impor_nsw_lga_suburb_func
def import_NSW_LGA_SUBURB(**kwargs):
    file_name= 'NSW_LGA_SUBURB.csv'
    file_path = os.path.join(LGA, file_name)
    df = pd.read_csv(file_path)
    selected_columns = ['LGA_NAME', 'SUBURB_NAME']
    df = df[selected_columns]
    if not df.empty:
        col_names = list(df.columns)  # Use all columns
        insert_sql = f"""
        INSERT INTO raw.NSW_LGA_SUBURB ({', '.join(col_names)})
                                VALUES %s
                                """
     
        values = [list(row) for row in df[col_names].itertuples(index=False)] 

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        print(f"Inserted {len(df)} records into the database.")
        conn_ps.commit()

    else:
        print("No records to insert.")
    
#Setup Python Operators to run the functions
import_listings_task = PythonOperator(
    task_id="import_listings_id",
    python_callable=import_listings,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_2016Census_G01_NSW_LGA_task = PythonOperator(
    task_id="import_2016Census_G01_NSW_LGA_id",
    python_callable=import_2016Census_G01_NSW_LGA,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_2016Census_G02_NSW_LGA_task = PythonOperator(
    task_id="import_2016Census_G02_NSW_LGA_id",
    python_callable=import_2016Census_G02_NSW_LGA,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_NSW_LGA_CODE_task = PythonOperator(
    task_id="import_NSW_LGA_CODE_id",
    python_callable=import_NSW_LGA_CODE,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_NSW_LGA_SUBURB_task = PythonOperator(
    task_id="import_NSW_LGA_SUBURB_id",
    python_callable=import_NSW_LGA_SUBURB,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Set up dependencies

[import_listings_task, import_2016Census_G01_NSW_LGA_task, import_2016Census_G02_NSW_LGA_task, import_NSW_LGA_CODE_task , import_NSW_LGA_SUBURB_task]