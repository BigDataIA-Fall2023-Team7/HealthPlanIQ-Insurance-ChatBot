import datetime
import requests
import zipfile
import io
import os
from datetime import datetime
import pandas as pd
import csv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.models import Variable

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# To solve the stuck requests problem on MacOS while developing
try:
    from _scproxy import _get_proxy_settings
    _get_proxy_settings()
except:
    pass


# Global Variables
PIPELINE_NAME='load_insurance_plan_benefits'
FILE_CACHE = os.getcwd() + Variable.get("airflow_var_filecache_usa_qhp")
GCS_BUCKET = Variable.get("airflow_var_gcsbucket_usa_qhp")

# TODO:
# FILE_CACHE = Variable.get("airflow_var_filecache")



#Utitlity Functions

def saveDFToCSV(csv_file_name, df):
    print(f"Saving as CSV file at {FILE_CACHE} location")
    destination_of_clean_csv = os.path.join(FILE_CACHE,csv_file_name)
    df.to_csv(destination_of_clean_csv, index=False, header= True, quoting=csv.QUOTE_ALL)
    print(f"Saved CSV file at {destination_of_clean_csv} location")
    return destination_of_clean_csv

  
def task_downloadInsurancePlanBenefitsCSVFile(**context):

    insurance_plan_benefits_dataset_url = context['params']['insurance_plan_benefits_dataset_url']
    insurance_plan_year = context['params']['insurance_plan_year']

    print(f"Downloading the insurance plan benefits dataset from url {insurance_plan_benefits_dataset_url}")
    file = requests.get(insurance_plan_benefits_dataset_url)

    if file.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(file.content)) as zip_file:
            file_name = zip_file.namelist()[0]
            print(f"Current Location : {os.getcwd()}")
            extracted_path = os.path.join(FILE_CACHE, file_name)
            zip_file.extractall(FILE_CACHE)
    
        print(f"Download complete!")
        print(f"Unzipping file and renaming the csv file inside it...")
        new_file_name = f"InsurancePlanBenefits_{insurance_plan_year}.csv"
        new_file_path = os.path.join(FILE_CACHE, new_file_name)
        os.rename(extracted_path, new_file_path)
        print(f"Unzipping file and renaming the csv file inside it complete!")

    context['ti'].xcom_push(key='downloadedInsurancePlanBenefitsCSVFile', value=new_file_path)


def task_cleanCSVFile(**context):
    ti = context['ti']
    insurance_plan_year = context['params']['insurance_plan_year']
    downloadedInsurancePlanBenefitsCSVFile = ti.xcom_pull(key='downloadedInsurancePlanBenefitsCSVFile')

    print(f"Downloaded the CSV FILE at {downloadedInsurancePlanBenefitsCSVFile} location")
    print(f"Cleaning the CSV FILE at {downloadedInsurancePlanBenefitsCSVFile} location")

    #Reading the dataset from excel file
    print(f"Accessing the CSV FILE at {downloadedInsurancePlanBenefitsCSVFile} location")
    insurance_plan_benefits = pd.read_csv(downloadedInsurancePlanBenefitsCSVFile)

    #Selecting only required columns
    required_cols = [
        'BusinessYear',
        'StateCode',
        'StandardComponentId',
        'BenefitName',
        'IsCovered'
    ]

    print(f"Selected only required columns from the CSV FILE at {downloadedInsurancePlanBenefitsCSVFile} location")
    insurance_plan_benefits = insurance_plan_benefits[required_cols]

    #Cleaning the columns
    
    
    #Remove the rows if nulls present in important columns
    # ['BusinessYear','StateCode', 'StandardComponentId'] -> important because they are table-joining key columns
    # 'BenefitName' -> important because we are building a benefits table
    insurance_plan_benefits.dropna(subset=['BusinessYear','StateCode', 'StandardComponentId', 'BenefitName'], inplace=True)

    #['IsCovered']
    insurance_plan_benefits['IsCovered'].fillna('Not Covered', inplace=True)

    #Get distinct benefits for the given ['BusinessYear','StateCode', 'StandardComponentId', 'BenefitName']
    insurance_plan_benefits = insurance_plan_benefits.drop_duplicates(subset=['BusinessYear', 'StateCode', 'StandardComponentId', 'BenefitName'])


    #Renaming columns for chatbot engine for easier inference
    rename_map = {
        'BusinessYear':'year',
        'StateCode':'state_code_for_insurance_issuer',
        'StandardComponentId':'insurance_plan_id',
        'BenefitName':'insurance_plan_benefit_name',
        'IsCovered':'is_insurance_plan_benefit_covered'
    }
    print(f"Renamed the required columns from the CSV FILE at {downloadedInsurancePlanBenefitsCSVFile} location")
    insurance_plan_benefits = insurance_plan_benefits.rename(columns=rename_map)




    #Saving the csv file locally in FILECACHE
    a = f"InsurancePlanBenefitsCoverage_{insurance_plan_year}.csv"
    d = saveDFToCSV(csv_file_name=a, df=insurance_plan_benefits)

    #Set XCOMs for GCS Uploading tasks
    gcs_prefix = 'insuranceplan_benefits'

    context['ti'].xcom_push(key='local_insurance_plan_benefits', value=d)
    context['ti'].xcom_push(key='bo_insurance_plan_benefits', value=f"{gcs_prefix}/{a}")
    context['ti'].xcom_push(key='insurance_plan_benefits_csvfilename', value=a)


with DAG(
    dag_id="load_insurance_plan_benefits",
    start_date=days_ago(1),
    schedule_interval=None,
    params={
    "insurance_plan_year": Param(2024, type="number"),
    "insurance_plan_benefits_dataset_url": Param("https://download.cms.gov/marketplace-puf/2024/benefits-and-cost-sharing-puf.zip", type="string")
    },
) as dag:

    downloadInsurancePlanBenefitsCSVFile = PythonOperator(
        task_id = 'downloadInsurancePlanBenefitsCSVFile',
        python_callable=task_downloadInsurancePlanBenefitsCSVFile,
        provide_context=True
    )

    cleanCSVFile = PythonOperator(
        task_id = 'cleanCSVFile',
        python_callable=task_cleanCSVFile,
    )

    uploadInsurancePlanBenefits_GCS = LocalFilesystemToGCSOperator(
        task_id = 'uploadInsurancePlanBenefits_GCS',
        src= "{{ ti.xcom_pull(task_ids='cleanCSVFile', key='local_insurance_plan_benefits') }}",
        dst= "{{ ti.xcom_pull(task_ids='cleanCSVFile', key='bo_insurance_plan_benefits') }}",
        bucket= GCS_BUCKET,
        mime_type="application/octet-stream",
        gcp_conn_id="google_cloud_default",
    )

    syncDataWithSFTable = SnowflakeOperator(
        task_id='syncDataWithSFTable',
        sql="CALL load_insurance_plan_benefits('{{ ti.xcom_pull(task_ids='cleanCSVFile', key='insurance_plan_benefits_csvfilename') }}');",  
        snowflake_conn_id='snowflake_default',  
        autocommit=True,
    )

    downloadInsurancePlanBenefitsCSVFile >> \
        cleanCSVFile >> \
            uploadInsurancePlanBenefits_GCS >> \
                syncDataWithSFTable
        