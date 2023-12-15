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
PIPELINE_NAME='load_insurance_plan_quality_ratings'
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

  
def task_downloadInsuranceQualityRatingsEXCELFile(**context):

    insurance_plan_quality_ratings_dataset_url = context['params']['insurance_plan_quality_ratings_dataset_url']
    insurance_plan_year = context['params']['insurance_plan_year']

    print(f"Downloading the insurance ratings dataset from url {insurance_plan_quality_ratings_dataset_url}")
    file = requests.get(insurance_plan_quality_ratings_dataset_url)

    if file.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(file.content)) as zip_file:

            quality_ratings_csvfilename = ""
            for files in zip_file.namelist():
                file_name, file_extension = os.path.splitext(files)
                if file_extension == '.csv':
                    quality_ratings_csvfilename = files
                    break
            
            print(f"Current Location : {os.getcwd()}")
            extracted_path = os.path.join(FILE_CACHE, quality_ratings_csvfilename)
            zip_file.extractall(FILE_CACHE)
    
        print(f"Download complete!")
        print(f"Unzipping file and renaming the csv file inside it...")
        new_file_name = f"InsurancePlanQualityRatings_{insurance_plan_year}.csv"
        new_file_path = os.path.join(FILE_CACHE, new_file_name)
        os.rename(extracted_path, new_file_path)
        print(f"Unzipping file and renaming the csv file inside it complete!")

    context['ti'].xcom_push(key='downloadedInsuranceQualityRatingsCSVFile', value=new_file_path)


def task_cleanCSVFile(**context):
    ti = context['ti']
    insurance_plan_year = context['params']['insurance_plan_year']
    downloadedInsuranceQualityRatingsCSVFile = ti.xcom_pull(key='downloadedInsuranceQualityRatingsCSVFile')

    print(f"Downloaded the CSV FILE at {downloadedInsuranceQualityRatingsCSVFile} location")
    print(f"Cleaning the CSV FILE at {downloadedInsuranceQualityRatingsCSVFile} location")

    #Reading the dataset from excel file
    print(f"Accessing the CSV FILE at {downloadedInsuranceQualityRatingsCSVFile} location")
    insurance_plan_quality_ratings = pd.read_csv(downloadedInsuranceQualityRatingsCSVFile)

    #Selecting only required columns
    required_cols = [
        'PlanID',
        'OverallRatingValue',
        'MedicalCareRatingValue',
        'MemberExperienceRatingValue',
        'PlanAdministrationRatingValue'
    ]

    print(f"Selected only required columns from the CSV FILE at {downloadedInsuranceQualityRatingsCSVFile} location")
    insurance_plan_quality_ratings = insurance_plan_quality_ratings[required_cols]

    #Cleaning the columns
    
    insurance_plan_quality_ratings.dropna(subset=['PlanID'], inplace=True)


    insurance_plan_quality_ratings['OverallRatingValue'] = insurance_plan_quality_ratings['OverallRatingValue'].replace('NR', None)
    insurance_plan_quality_ratings['MedicalCareRatingValue'] = insurance_plan_quality_ratings['MedicalCareRatingValue'].replace('NR', None)
    insurance_plan_quality_ratings['MemberExperienceRatingValue'] = insurance_plan_quality_ratings['MemberExperienceRatingValue'].replace('NR', None)
    insurance_plan_quality_ratings['PlanAdministrationRatingValue'] = insurance_plan_quality_ratings['PlanAdministrationRatingValue'].replace('NR', None)
    

    insurance_plan_quality_ratings = insurance_plan_quality_ratings.drop_duplicates(subset=['PlanID'])


    #Renaming columns for chatbot engine for easier inference
    rename_map = {
        'PlanID':'insurance_plan_id',
        'OverallRatingValue':'insurance_plan_overall_quality_rating',
        'MedicalCareRatingValue':'insurance_plan_quality_rating_for_medical_care',
        'MemberExperienceRatingValue':'insurance_plan_quality_rating_for_member_experience',
        'PlanAdministrationRatingValue':'insurance_plan_quality_rating_for_plan_administration'
    }
    print(f"Renamed the required columns from the CSV FILE at {downloadedInsuranceQualityRatingsCSVFile} location")
    insurance_plan_quality_ratings = insurance_plan_quality_ratings.rename(columns=rename_map)

    insurance_plan_quality_ratings['year'] = insurance_plan_year




    #Saving the csv file locally in FILECACHE
    a = f"InsurancePlanQualityRatings_{insurance_plan_year}.csv"
    d = saveDFToCSV(csv_file_name=a, df=insurance_plan_quality_ratings)

    #Set XCOMs for GCS Uploading tasks
    gcs_prefix = 'insuranceplan_quality_ratings'

    context['ti'].xcom_push(key='local_insurance_plan_quality_ratings', value=d)
    context['ti'].xcom_push(key='bo_insurance_plan_quality_ratings', value=f"{gcs_prefix}/{a}")
    context['ti'].xcom_push(key='insurance_plan_quality_ratings_csvfilename', value=a)


with DAG(
    dag_id="load_insurance_plan_quality_ratings",
    start_date=days_ago(1),
    schedule_interval=None,
    params={
    "insurance_plan_year": Param(2024, type="number"),
    "insurance_plan_quality_ratings_dataset_url": Param("https://www.cms.gov/files/zip/quality-puf-py2024.zip", type="string")
    },
) as dag:

    downloadInsuranceQualityRatingsEXCELFile = PythonOperator(
        task_id = 'downloadInsuranceQualityRatingsEXCELFile',
        python_callable=task_downloadInsuranceQualityRatingsEXCELFile,
        provide_context=True
    )

    cleanCSVFile = PythonOperator(
        task_id = 'cleanCSVFile',
        python_callable=task_cleanCSVFile,
    )

    uploadInsurancePlanQualityRatings_GCS = LocalFilesystemToGCSOperator(
        task_id = 'uploadInsurancePlanQualityRatings_GCS',
        src= "{{ ti.xcom_pull(task_ids='cleanCSVFile', key='local_insurance_plan_quality_ratings') }}",
        dst= "{{ ti.xcom_pull(task_ids='cleanCSVFile', key='bo_insurance_plan_quality_ratings') }}",
        bucket= GCS_BUCKET,
        mime_type="application/octet-stream",
        gcp_conn_id="google_cloud_default",
    )

    syncDataWithSFTable = SnowflakeOperator(
        task_id='syncDataWithSFTable',
        sql="CALL load_insurance_plan_quality_ratings('{{ ti.xcom_pull(task_ids='cleanCSVFile', key='insurance_plan_quality_ratings_csvfilename') }}');",  
        snowflake_conn_id='snowflake_default',  
        autocommit=True,
    )

    downloadInsuranceQualityRatingsEXCELFile >> \
        cleanCSVFile >> \
            uploadInsurancePlanQualityRatings_GCS >> \
                syncDataWithSFTable
        