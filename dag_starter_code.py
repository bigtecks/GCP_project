# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators
# Import Libraries to handle dataframes
# Import Library to send Slack Messages
# Import Library to set DAG timing

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths
FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/'
# Import CSV into a pandas dataframe
# Slack webhook link
slack_webhook = ' '

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send messages over slack using the slack_webhook
def send_msg(text_string):
    """
    This function uses the requests library to send messages
    on Slack using a webhook
    """  

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

# --------------------------------
# DAG definition
# --------------------------------

# Define the defualt args
default_args = {}
# Create the DAG object
with DAG() as dag:
    # start_task
    # file_sensor_task
    # send_report_task
    # flag_anomaly_task
    # end_task
# Set the dependencies

# --------------------------------