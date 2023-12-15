import datetime
import requests
import zipfile
import io
import os
from datetime import datetime
import pandas as pd
import csv
import ssl
import urllib3
import json
import hashlib

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
PIPELINE_NAME='load_insurance_plan_covered_drugs'
FILE_CACHE = os.getcwd() + Variable.get("airflow_var_filecache_usa_qhp")
GCS_BUCKET = Variable.get("airflow_var_gcsbucket_usa_qhp")

# TODO:
# FILE_CACHE = Variable.get("airflow_var_filecache")



#Utitlity Functions

def generate_sha256_hash(input_string):
    sha256_hash = hashlib.sha256(input_string.encode()).hexdigest()
    return sha256_hash

def saveJSON(json_file_name, jsonData):
    print(f"Saving as JSON file at {FILE_CACHE} location")
    destination_of_clean_json = os.path.join(FILE_CACHE,json_file_name)
    with open(destination_of_clean_json, 'w') as f:
        json.dump(jsonData, f)
    print(f"Saved JSON file at {destination_of_clean_json} location")
    return destination_of_clean_json


class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)
        
def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session

def saveDFToCSV(csv_file_name, df):
    print(f"Saving as CSV file at {FILE_CACHE} location")
    destination_of_clean_csv = os.path.join(FILE_CACHE,csv_file_name)
    df.to_csv(destination_of_clean_csv, index=False, header= True, quoting=csv.QUOTE_ALL)
    print(f"Saved CSV file at {destination_of_clean_csv} location")
    return destination_of_clean_csv

  
def task_downloadInsuranceIssuerMachineReadableCSVFile(**context):

    insurance_issuer_machine_readable_url = context['params']['insurance_issuer_machine_readable_url']

    print(f"Downloading the insurance issuer machine readable dataset from url {insurance_issuer_machine_readable_url}")
    file = requests.get(insurance_issuer_machine_readable_url)

    if file.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(file.content)) as zip_file:
            file_name = zip_file.namelist()[0]
            print(f"Current Location : {os.getcwd()}")
            extracted_path = os.path.join(FILE_CACHE, file_name)
            zip_file.extractall(FILE_CACHE)
    
        print(f"Download complete!")
        print(f"Unzipping file and renaming the EXCEL file inside it...")
        new_file_name = f"InsuranceIssuerMachineReadable.xlsx"
        new_file_path = os.path.join(FILE_CACHE, new_file_name)
        os.rename(extracted_path, new_file_path)
        print(f"Unzipping file and renaming the EXCEL file inside it complete!")

    context['ti'].xcom_push(key='downloadedInsuranceIssuerMachineReadableExcelFile', value=new_file_path)


def task_extractStateSpecificMRURLS(**context):
    ti = context['ti']
    state = context['params']['state']
    downloadedInsuranceIssuerMachineReadableExcelFile = ti.xcom_pull(key='downloadedInsuranceIssuerMachineReadableExcelFile')

    print(f"Downloaded the EXCEL FILE at {downloadedInsuranceIssuerMachineReadableExcelFile} location")

    #Reading the dataset from excel file
    print(f"Accessing the EXCEL FILE at {downloadedInsuranceIssuerMachineReadableExcelFile} location")
    print(f"Selecting the state {state} records from the dataset for further processing")
    insurance_issuer_mr_dataset = pd.read_excel(downloadedInsuranceIssuerMachineReadableExcelFile)

    #Selecting subset from main dataset - to get only selected state records
    state_specific_mr = insurance_issuer_mr_dataset[insurance_issuer_mr_dataset['State']==state]

    #Selecting only required columns
    required_cols = [
        'State',
        'Issuer ID',
        'URL Submitted'
    ]

    print(f"Selected only required columns from the CSV FILE at {downloadedInsuranceIssuerMachineReadableExcelFile} location")
    state_specific_mr = state_specific_mr[required_cols]

    #Cleaning the columns
    state_specific_mr.dropna(subset=['State','Issuer ID', 'URL Submitted'], inplace=True)

    context['ti'].xcom_push(key='state_specific_insurance_issuer_mr_urls', value=state_specific_mr.to_dict(orient='records'))


def task_downloadDrugsJSONData(**context):
    ti = context['ti']
    state_specific_insurance_issuer_mr_urls = ti.xcom_pull(key='state_specific_insurance_issuer_mr_urls')

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }

    saved_drug_json_locations = []
    for state_mr_record in state_specific_insurance_issuer_mr_urls:
        print(f"State : {state_mr_record['State']}, DiscoveryURL : {state_mr_record['URL Submitted']}")
        response = get_legacy_session().get(state_mr_record['URL Submitted'], headers=headers)
        if response.status_code == 200:
            b = response.json()
            if 'formulary_urls' in b:
                if len(b['formulary_urls']) !=0:
                    for drugUrl in b['formulary_urls']:
                        print(f"Drugs URL : {drugUrl}")
                        a = get_legacy_session().get(drugUrl, headers=headers)
                        if a.status_code==200:
                            drugsCoveredDataset_JSON = a.json()
                            urlHash = generate_sha256_hash(drugUrl)
                            file_name = f"{state_mr_record['State']}_{state_mr_record['Issuer ID']}_{urlHash}.json"
                            saved_json_location = saveJSON(json_file_name=file_name, jsonData=drugsCoveredDataset_JSON)
                            saved_drug_json_locations.append(saved_json_location)

    context['ti'].xcom_push(key='saved_drug_json_locations', value=saved_drug_json_locations)
    

def task_mapDrugJSONToCSV(**context):
    ti = context['ti']
    saved_drug_json_locations = ti.xcom_pull(key='saved_drug_json_locations')


    saved_drug_csv_locations = []
    
    for drugJSONLocation in saved_drug_json_locations:

        records = []

        #Read the locally saved json file
        print(f"Reading locally saved drug JSON file {drugJSONLocation}")
        with open(drugJSONLocation, 'r') as file:
            drugJSONData = json.load(file)


        print(f"Mapping Drug JSON to CSV format")
        #Extracting json elements for drugs.csv file
        if len(drugJSONData)!=0:
            for drug in drugJSONData:
                rxnorm_id = drug.get('rxnorm_id', None)
                drug_name = drug.get('drug_name', None)
                if drug.get('plans', None):
                    for supportingPlans in drug['plans']:
                        supportingPlanId = supportingPlans.get('plan_id', None)
                        drug_tier = supportingPlans.get('drug_tier', None)
                        if supportingPlans.get('years', None):
                            for year in supportingPlans['years']:
                                records.append({
                                                        'year':year,
                                                        'insurance_plan_id':supportingPlanId,
                                                        'rxnorm_drug_id':rxnorm_id,
                                                        'insurance_plan_covered_drug_name':drug_name,
                                                        'insurance_plan_covered_drug_tier':drug_tier
                                                })
    
        df_drugs_data = pd.DataFrame(records)
        csvfilename = os.path.basename(drugJSONLocation)[:-5] + ".csv"
        local_drug_csv_file = saveDFToCSV(csv_file_name=csvfilename, df=df_drugs_data)
        print(f"Mapped {drugJSONLocation} JSON File to {local_drug_csv_file} CSV File")
        saved_drug_csv_locations.append({'csvfilename': csvfilename, 'location_drug_csv_file': local_drug_csv_file})

    context['ti'].xcom_push(key='saved_drug_csv_locations', value=saved_drug_csv_locations)


def task_uploadInsurancePlanWiseDrugCoverage_GCS(**context):
    ti = context['ti']
    saved_drug_csv_locations = ti.xcom_pull(key='saved_drug_csv_locations')    

    for i, row in enumerate(saved_drug_csv_locations):

        csvfilename = row['csvfilename']
        location_drug_csv_file = row['location_drug_csv_file']

        gcs_bucket = GCS_BUCKET
        gcs_prefix = 'insuranceplan_drug_coverage'
        gcs_object_name = f"{gcs_prefix}/{csvfilename}"
        upload_task = LocalFilesystemToGCSOperator(
                        task_id=f'{str(i)}_{csvfilename}',
                        src=location_drug_csv_file,
                        dst=gcs_object_name,
                        bucket=gcs_bucket,
                        mime_type="application/octet-stream",
                        gcp_conn_id="google_cloud_default",
                        dag=dag,
                )
        upload_task.execute(context=context)  
        print(f"Uploaded {csvfilename} to GCS {gcs_object_name}")                               
    


def task_stageDrugsCSVToSF(**context):
    ti = context['ti']
    saved_drug_csv_locations = ti.xcom_pull(key='saved_drug_csv_locations')   

    for i, row in enumerate(saved_drug_csv_locations):

        csvfilename = row['csvfilename']
        syncDataWithSFTable_stg_drug_coverage = SnowflakeOperator(
            task_id=f'syncDataWithSFTable_stg_drug_coverage_{str(i)}',
            sql=f"CALL load_stg_insurance_plan_drug_coverage('{csvfilename}');",  
            snowflake_conn_id='snowflake_default',  
            autocommit=True,
        )
        syncDataWithSFTable_stg_drug_coverage.execute(context=context)
        print(f"Staged {csvfilename} to SF Drugs stage table")
    






with DAG(
    dag_id="load_insurance_plan_covered_drugs",
    start_date=days_ago(1),
    schedule_interval=None,
    params={
    "state":Param("AK", type="string"),
    "insurance_issuer_machine_readable_url": Param("https://download.cms.gov/marketplace-puf/2024/machine-readable-url-puf.zip", type="string")
    }
) as dag:

    downloadInsuranceIssuerMachineReadableCSVFile = PythonOperator(
        task_id = 'downloadInsuranceIssuerMachineReadableCSVFile',
        python_callable=task_downloadInsuranceIssuerMachineReadableCSVFile,
        provide_context=True
    )

    extractStateSpecificMRURLS = PythonOperator(
        task_id = 'extractStateSpecificMRURLS',
        python_callable=task_extractStateSpecificMRURLS
    )

    downloadDrugsJSONData = PythonOperator(
        task_id = 'downloadDrugsJSONData',
        python_callable=task_downloadDrugsJSONData,
    )

    mapDrugJSONToCSV = PythonOperator(
        task_id = 'mapDrugJSONToCSV',
        python_callable=task_mapDrugJSONToCSV,
    )


    uploadInsurancePlanWiseDrugCoverage_GCS = PythonOperator(
        task_id = 'uploadInsurancePlanWiseDrugCoverage_GCS',
        python_callable=task_uploadInsurancePlanWiseDrugCoverage_GCS,
    )

    stageDrugsCSVToSF = PythonOperator(
        task_id = 'stageDrugsCSVToSF',
        python_callable=task_stageDrugsCSVToSF,
    )

    mergeDrugsInSF = SnowflakeOperator(
        task_id='mergeDrugsInSF',
        sql="CALL merge_stg_to_insurance_plan_drug_coverage();",  
        snowflake_conn_id='snowflake_default',  
        autocommit=True,
    )


    downloadInsuranceIssuerMachineReadableCSVFile >> \
        extractStateSpecificMRURLS >> \
            downloadDrugsJSONData >> \
                mapDrugJSONToCSV >> \
                    uploadInsurancePlanWiseDrugCoverage_GCS >> stageDrugsCSVToSF >> mergeDrugsInSF
        
