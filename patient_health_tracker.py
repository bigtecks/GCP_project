# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import BranchPythonOperator

# Import Libraries to handle dataframes
import pandas as pd
import numpy as np

# Import Library to send Slack Messages
import requests
import os

# Import Library to set DAG timing
from datetime import timedelta, datetime

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths
FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/'

# Import CSV into a pandas dataframe
#file_name = os.listdir(FILE_PATH_INPUT)[0]
df = pd.read_csv(FILE_PATH_INPUT + 'diag_metrics_P0015.csv')


# Slack webhook link
slack_webhook = 'https://hooks.slack.com/services/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'

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send messages over slack using the slack_webhook
def send_msg(text_string):
    """
    This function uses the requests library to send messages
    on Slack using a webhook
    """  
    requests.post(slack_webhook, json={'text': text_string}) 

# Function to generate the diagnostics report
# Add print statements to each variable so that it appears on the Logs
def send_report():
    """
    This function sends a disgnostics report to Slack with
    the below patient details:
    #1. Average O2 level
    #2. Average Heart Rate
    #3. Standard Deviation of O2 level
    #4. Standard Deviation of Heart Rate
    #5. Minimum O2 level
    #6. Minimum Heart Rate
    #7. Maximum O2 level
    #8. Maximum Heart Rate
    """
    avg_o2_level = df['o2_level'].mean()
    avg_heart_rate = df['heart_rate'].mean()
    std_o2_level = df['o2_level'].std()
    std_heart_rate = df['heart_rate'].std()
    min_o2_level = df['o2_level'].min()
    min_heart_rate = df['heart_rate'].min()
    max_o2_level = df['o2_level'].max()
    max_heart_rate = df['heart_rate'].max()

    print(avg_o2_level)
    print(avg_heart_rate)
    print(std_o2_level)
    print(std_heart_rate)
    print(min_o2_level)
    print(min_heart_rate)
    print(max_o2_level)
    print(max_heart_rate)

    text_string = 'Patient has:\n average o2 level of: {:.2f}\n average heart rate of: {:.2f}\n standard deviation o2 level of: {:.2f}\n standard deviation heart rate of: {:.2f}\n minimum o2 level of: {:.2f}\n minimum heart rate of: {:.2f}\n max o2 level of: {:.2f}\n max heart rate of: {:.2f}'.format(
        avg_o2_level,
        avg_heart_rate,
        std_o2_level,
        std_heart_rate,
        min_o2_level,
        min_heart_rate,
        max_o2_level,
        max_heart_rate
    )
    send_msg(text_string)

#3 Function to filter anomalies in the data
# Add print statements to each output dataframe so that it appears on the Logs
def flag_anomaly():
    """
    As per the patient's past medical history, below are the mu and sigma 
    for normal levels of heart rate and o2:
    
    Heart Rate = (80.81, 10.28)
    O2 Level = (96.19, 1.69)

    Only consider the data points which exceed the (mu + 3*sigma) or (mu - 3*sigma) levels as anomalies
    Filter and save the anomalies as 2 dataframes in the output folder - hr_anomaly_P0015.csv & o2_anomaly_P0015.csv
    """
    hr_anomaly_upper = df.loc[df['heart_rate'] > (80.81 + 10.28 * 3)]
    hr_anomaly_lower = df.loc[df['heart_rate'] < (80.81 - 10.28 * 3)]
    hr_anomaly_lower_upper = [hr_anomaly_lower, hr_anomaly_upper]
    hr_anomaly = pd.concat(hr_anomaly_lower_upper)
    print(hr_anomaly)
    hr_anomaly.to_csv(FILE_PATH_OUTPUT + 'hr_anomaly_P0015.csv', index=False)

    o2_anomaly_upper = df.loc[df['o2_level'] > (96.19 + 1.69 * 3)]
    o2_anomaly_lower = df.loc[df['o2_level'] < (96.19 - 1.69 * 3)]
    o2_anomaly_lower_upper = [o2_anomaly_lower, o2_anomaly_upper]
    o2_anomaly = pd.concat(o2_anomaly_lower_upper)
    print(o2_anomaly)
    o2_anomaly.to_csv(FILE_PATH_OUTPUT + 'o2_anomaly_P0015.csv', index=False)



# --------------------------------
# DAG definition
# --------------------------------

# Define the default args
default_args = {
    'owner': 'user_name',
    'start_date': datetime(2022, 5, 5),
    'depends_on_past': False,
    'email': ['bigtecks1@gmail.com'], 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
# Create the DAG object
with DAG(
    'patient_health_tracker',
    default_args=default_args,
    description='DAG',
    schedule_interval='*/15 * * * *',  #*/15 * * * * every 15 minutes
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    file_sensor = FileSensor(
        task_id='file_sensor',
        poke_interval=15,           #every 15 minutes
        filepath=FILE_PATH_INPUT,
        timeout=5,
        dag=dag
    )

    send_report_task = PythonOperator(
        task_id='send_report',
        python_callable=send_report,
        dag=dag
    )

    flag_anomaly_task = PythonOperator(
        task_id='flag_anomaly',
        python_callable=flag_anomaly,
        dag=dag
    )

    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )
    
    
    
    # start_task
    # file_sensor_task
    # send_report_task
    # flag_anomaly_task
    # end_task


# Set the dependencies

start_task >> file_sensor >> [send_report_task,flag_anomaly_task]
flag_anomaly_task >> end_task
send_report_task >> end_task

# --------------------------------