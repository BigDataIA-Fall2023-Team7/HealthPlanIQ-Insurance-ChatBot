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
PIPELINE_NAME='load_insurance_plans'
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

  
def task_downloadInsurancePlansExcelFile(**context):
    # ti = context['ti']
    # insurance_plan_dataset_url = ti.xcom_pull(key='insurance_plan_dataset_url')
    # insurance_plan_year = ti.xcom_pull(key='insurance_plan_year')
    # context['ti'].xcom_push(key='gcsSavedPDFLocations', value=gcsSavedPDFLocations)

    insurance_plan_dataset_url = context['params']['insurance_plan_dataset_url']
    insurance_plan_year = context['params']['insurance_plan_year']

    print(f"Downloading the insurance plan dataset from url {insurance_plan_dataset_url}")
    file = requests.get(insurance_plan_dataset_url)

    if file.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(file.content)) as zip_file:
            file_name = zip_file.namelist()[0]
            print(f"Current Location : {os.getcwd()}")
            extracted_path = os.path.join(FILE_CACHE, file_name)
            zip_file.extractall(FILE_CACHE)
    
        print(f"Download complete!")
        print(f"Unzipping file and renaming the excel file inside it...")
        new_file_name = f"InsurancePlans_{insurance_plan_year}.xlsx"
        new_file_path = os.path.join(FILE_CACHE, new_file_name)
        os.rename(extracted_path, new_file_path)
        print(f"Unzipping file and renaming the excel file inside it complete!")

    context['ti'].xcom_push(key='downloadedInsurancePlansExcelFile', value=new_file_path)


def task_cleanAndConvertToCSVFile(**context):
    ti = context['ti']
    insurance_plan_year = context['params']['insurance_plan_year']
    downloadedInsurancePlansExcelFile = ti.xcom_pull(key='downloadedInsurancePlansExcelFile')

    print(f"Downloaded the EXCEL FILE at {downloadedInsurancePlansExcelFile} location")
    print(f"Cleaning the EXCEL FILE at {downloadedInsurancePlansExcelFile} location")

    #Reading the dataset from excel file
    print(f"Accessing the EXCEL FILE at {downloadedInsurancePlansExcelFile} location")
    insurance_plans = pd.read_excel(downloadedInsurancePlansExcelFile, header=1)

    #Selecting only required columns
    required_cols = [
        'State Code',
        'County Name',
        'Metal Level',
        'Issuer Name',
        'HIOS Issuer ID',
        'Plan ID (Standard Component)',
        'Plan Marketing Name',
        'Plan Type',
        'Rating Area',
        'Customer Service Phone Number Local',
        'Customer Service Phone Number Toll Free',
        'Customer Service Phone Number TTY',
        'Network URL',
        'Plan Brochure URL',
        'Summary of Benefits URL',
        'Drug Formulary URL',
        'Adult Dental ',
        'Child Dental ',
        'Premium Child Age 0-14',
        'Premium Child Age 18',
        'Premium Adult Individual Age 21',
        'Premium Adult Individual Age 27',
        'Premium Adult Individual Age 30 ',
        'Premium Adult Individual Age 40 ',
        'Premium Adult Individual Age 50 ',
        'Premium Adult Individual Age 60 ',
        'Premium Couple 21  ',
        'Premium Couple 30 ',
        'Premium Couple 40 ',
        'Premium Couple 50 ',
        'Premium Couple 60 ',
        'Couple+1 child, Age 21',
        'Couple+1 child, Age 30 ',
        'Couple+1 child, Age 40 ',
        'Couple+1 child, Age 50 ',
        'Couple+2 children, Age 21',
        'Couple+2 children, Age 30 ',
        'Couple+2 children, Age 40 ',
        'Couple+2 children, Age 50',
        'Couple+3 or more Children, Age 21',
        'Couple+3 or more Children, Age 30',
        'Couple+3 or more Children, Age 40',
        'Couple+3 or more Children, Age 50',
        'Individual+1 child, Age 21',
        'Individual+1 child, Age 30',
        'Individual+1 child, Age 40',
        'Individual+1 child, Age 50',
        'Individual+2 children, Age 21',
        'Individual+2 children, Age 30',
        'Individual+2 children, Age 40',
        'Individual+2 children, Age 50',
        'Individual+3 or more children, Age 21',
        'Individual+3 or more children, Age 30',
        'Individual+3 or more children, Age 40',
        'Individual+3 or more children, Age 50',
        'Medical Deductible - Individual - Standard',
        'Drug Deductible - Individual - Standard',
        'Medical Deductible - Family - Standard',
        'Drug Deductible - Family - Standard',
        'Medical Deductible - Family (Per Person) - Standard',
        'Drug Deductible - Family (Per Person) - Standard',
        'Medical Maximum Out Of Pocket - Individual - Standard',
        'Drug Maximum Out Of Pocket - Individual - Standard',
        'Medical Maximum Out Of Pocket - Family - Standard',
        'Drug Maximum Out Of Pocket - Family - Standard',
        'Medical Maximum Out Of Pocket - Family (Per Person) - Standard',
        'Drug Maximum Out Of Pocket - Family (Per Person) - Standard',
        'Primary Care Physician - Standard',
        'Specialist - Standard',
        'Emergency Room - Standard',
        'Inpatient Facility - Standard',
        'Inpatient Physician - Standard',
        'Generic Drugs - Standard',
        'Preferred Brand Drugs - Standard',
        'Non-preferred Brand Drugs - Standard',
        'Specialty Drugs - Standard'
    ]

    print(f"Selected only required columns from the EXCEL FILE at {downloadedInsurancePlansExcelFile} location")
    insurance_plans = insurance_plans[required_cols]

    #Cleaning the columns
    
    #['Adult Dental ']
    insurance_plans['Adult Dental '].fillna(False, inplace=True)
    insurance_plans['Adult Dental '] = insurance_plans['Adult Dental '].str.lower() == 'x'
    
    #['Child Dental ']
    insurance_plans['Child Dental '].fillna(False, inplace=True)
    insurance_plans['Child Dental '] = insurance_plans['Child Dental '].str.lower() == 'x'


    #Renaming columns for chatbot engine for easier inference
    rename_map = {
        'State Code' : 'state_code_for_insurance_issuer',
        'County Name' : 'county_name_for_insurance_issuer',
        'Metal Level' : 'insurance_plan_level',
        'Issuer Name' : 'insurance_issuer_name',
        'HIOS Issuer ID' : 'insurance_issuer_id',
        'Plan ID (Standard Component)' : 'insurance_plan_id',
        'Plan Marketing Name' : 'insurance_plan_name',
        'Plan Type' : 'insurance_plan_type',
        'Rating Area' : 'rating_area_for_insurance_plan',
        'Customer Service Phone Number Local' : 'customer_service_phone_number',
        'Customer Service Phone Number Toll Free' : 'tollfree_customer_service_phone_number',
        'Customer Service Phone Number TTY' : 'tty_customer_service_phone_number',
        'Network URL' : 'url_for_insurance_issuer_network',
        'Plan Brochure URL' : 'url_for_insurance_plan_brochure',
        'Summary of Benefits URL' : 'url_for_summary_of_insurance_benefits',
        'Drug Formulary URL' : 'url_for_drugs_supported_by_insurance_plan',
        'Adult Dental ' : 'is_adult_dental_covered',
        'Child Dental ' : 'is_child_dental_covered',
        'Premium Child Age 0-14' : 'premium_for_child_with_age_between_0_and_14',
        'Premium Child Age 18' : 'premium_for_child_with_age_between_15_and_18',
        'Premium Adult Individual Age 21' : 'premium_for_adult_with_age_between_19_and_21',
        'Premium Adult Individual Age 27' : 'premium_for_adult_with_age_between_22_and_27',
        'Premium Adult Individual Age 30 ' : 'premium_for_adult_with_age_between_28_and_30',
        'Premium Adult Individual Age 40 ' : 'premium_for_adult_with_age_between_31_and_40',
        'Premium Adult Individual Age 50 ' : 'premium_for_adult_with_age_between_41_and_50',
        'Premium Adult Individual Age 60 ' : 'premium_for_adult_with_age_between_51_and_60',
        'Premium Couple 21  ' : 'premium_for_couple_with_one_age_between_19_and_21',
        'Premium Couple 30 ' : 'premium_for_couple_with_one_age_between_22_and_30',
        'Premium Couple 40 ' : 'premium_for_couple_with_one_age_between_31_and_40',
        'Premium Couple 50 ' : 'premium_for_couple_with_one_age_between_41_and_50',
        'Premium Couple 60 ' : 'premium_for_couple_with_one_age_between_51_and_60',
        'Couple+1 child, Age 21' : 'premium_for_couple_with_one_child_with_one_adult_age_between_19_and_21',
        'Couple+1 child, Age 30 ' : 'premium_for_couple_with_one_child_with_one_adult_age_between_22_and_30',
        'Couple+1 child, Age 40 ' : 'premium_for_couple_with_one_child_with_one_adult_age_between_31_and_40',
        'Couple+1 child, Age 50 ' : 'premium_for_couple_with_one_child_with_one_adult_age_between_41_and_50',
        'Couple+2 children, Age 21' : 'premium_for_couple_with_two_children_with_one_adult_age_between_19_and_21',
        'Couple+2 children, Age 30 ' : 'premium_for_couple_with_two_children_with_one_adult_age_between_22_and_30',
        'Couple+2 children, Age 40 ' : 'premium_for_couple_with_two_children_with_one_adult_age_between_31_and_40',
        'Couple+2 children, Age 50' : 'premium_for_couple_with_two_children_with_one_adult_age_between_41_and_50',
        'Couple+3 or more Children, Age 21' : 'premium_for_couple_with_three_children_with_one_adult_age_between_19_and_21',
        'Couple+3 or more Children, Age 30' : 'premium_for_couple_with_three_children_with_one_adult_age_between_22_and_30',
        'Couple+3 or more Children, Age 40' : 'premium_for_couple_with_three_children_with_one_adult_age_between_31_and_40',
        'Couple+3 or more Children, Age 50' : 'premium_for_couple_with_three_children_with_one_adult_age_between_41_and_50',
        'Individual+1 child, Age 21' : 'premium_for_single_parent_with_one_child_with_parent_age_between_19_and_21',
        'Individual+1 child, Age 30' : 'premium_for_single_parent_with_one_child_with_parent_age_between_22_and_30',
        'Individual+1 child, Age 40' : 'premium_for_single_parent_with_one_child_with_parent_age_between_31_and_40',
        'Individual+1 child, Age 50' : 'premium_for_single_parent_with_one_child_with_parent_age_between_41_and_50',
        'Individual+2 children, Age 21' : 'premium_for_single_parent_with_two_children_with_parent_age_between_19_and_21',
        'Individual+2 children, Age 30' : 'premium_for_single_parent_with_two_children_with_parent_age_between_22_and_30',
        'Individual+2 children, Age 40' : 'premium_for_single_parent_with_two_children_with_parent_age_between_31_and_40',
        'Individual+2 children, Age 50' : 'premium_for_single_parent_with_two_children_with_parent_age_between_41_and_50',
        'Individual+3 or more children, Age 21' : 'premium_for_single_parent_with_three_children_with_parent_age_between_19_and_21',
        'Individual+3 or more children, Age 30' : 'premium_for_single_parent_with_three_children_with_parent_age_between_22_and_30',
        'Individual+3 or more children, Age 40' : 'premium_for_single_parent_with_three_children_with_parent_age_between_31_and_40',
        'Individual+3 or more children, Age 50' : 'premium_for_single_parent_with_three_children_with_parent_age_between_41_and_50',
        'Medical Deductible - Individual - Standard' : 'deductible_for_medical_services_offered_to_individual',
        'Drug Deductible - Individual - Standard' : 'deductible_for_prescription_drugs_offered_to_individual',
        'Medical Deductible - Family - Standard' : 'deductible_for_medical_services_offered_to_entire_family',
        'Drug Deductible - Family - Standard' : 'deductible_for_prescription_drugs_offered_to_entire_family',
        'Medical Deductible - Family (Per Person) - Standard' : 'deductible_for_medical_services_offered_per_person_in_family',
        'Drug Deductible - Family (Per Person) - Standard' : 'deductible_for_prescription_drugs_offered_per_person_in_family',
        'Medical Maximum Out Of Pocket - Individual - Standard' : 'maximum_out_of_pocket_charges_for_medical_services_offered_to_individual',
        'Drug Maximum Out Of Pocket - Individual - Standard' : 'maximum_out_of_pocket_charges_for_prescription_drugs_offered_to_individual',
        'Medical Maximum Out Of Pocket - Family - Standard' : 'maximum_out_of_pocket_charges_for_medical_services_offered_to_entire_family',
        'Drug Maximum Out Of Pocket - Family - Standard' : 'maximum_out_of_pocket_charges_for_prescription_drugs_offered_to_entire_family',
        'Medical Maximum Out Of Pocket - Family (Per Person) - Standard' : 'maximum_out_of_pocket_charges_for_medical_services_offered_per_person_in_family',
        'Drug Maximum Out Of Pocket - Family (Per Person) - Standard' : 'maximum_out_of_pocket_charges_for_prescription_drugs_offered_per_person_in_family',
        'Primary Care Physician - Standard' : 'copay_or_coinsurance_per_visit_to_primary_care_physician',
        'Specialist - Standard' : 'copay_or_coinsurance_per_visit_to_specialist',
        'Emergency Room - Standard' : 'copay_or_coinsurance_for_emergency_room_service',
        'Inpatient Facility - Standard' : 'copay_or_coinsurance_for_inpatient_facility_service',
        'Inpatient Physician - Standard' : 'copay_or_coinsurance_for_inpatient_physician_service',
        'Generic Drugs - Standard' : 'copay_or_coinsurance_per_prescription_having_generic_drugs',
        'Preferred Brand Drugs - Standard' : 'copay_or_coinsurance_per_prescription_having_preferred_brand_drugs',
        'Non-preferred Brand Drugs - Standard' : 'copay_or_coinsurance_per_prescription_having_non_preferred_brand_drugs',
        'Specialty Drugs - Standard' : 'copay_or_coinsurance_per_prescription_having_specialty_drugs'
    }

    print(f"Renamed the required columns from the EXCEL FILE at {downloadedInsurancePlansExcelFile} location")
    insurance_plans = insurance_plans.rename(columns=rename_map)

    #Adding the year column to the dataframe
    insurance_plans['year'] = insurance_plan_year


    #Splitting the datasets
    # 1. InsurancePlanDetails_<year>
    # 2. InsurancePlanPremiumRates_<year>
    # 3. InsurancePlanDeductibleRates_<year>
    
    insurance_plan_details_columns = [
        'year',
        'state_code_for_insurance_issuer',
        'county_name_for_insurance_issuer',
        'insurance_plan_id',
        'insurance_plan_name',
        'insurance_plan_type',
        'insurance_plan_level',
        'insurance_issuer_id',
        'insurance_issuer_name',
        'rating_area_for_insurance_plan',
        'customer_service_phone_number',
        'tollfree_customer_service_phone_number',
        'tty_customer_service_phone_number',
        'url_for_insurance_issuer_network',
        'url_for_insurance_plan_brochure',
        'url_for_summary_of_insurance_benefits',
        'url_for_drugs_supported_by_insurance_plan',
        'is_adult_dental_covered',
        'is_child_dental_covered'
    ]

    insurance_plan_premium_rates_columns = [
        'year',
        'state_code_for_insurance_issuer',
        'county_name_for_insurance_issuer',
        'insurance_plan_id',
        'premium_for_child_with_age_between_0_and_14',
        'premium_for_child_with_age_between_15_and_18',
        'premium_for_adult_with_age_between_19_and_21',
        'premium_for_adult_with_age_between_22_and_27',
        'premium_for_adult_with_age_between_28_and_30',
        'premium_for_adult_with_age_between_31_and_40',
        'premium_for_adult_with_age_between_41_and_50',
        'premium_for_adult_with_age_between_51_and_60',
        'premium_for_couple_with_one_age_between_19_and_21',
        'premium_for_couple_with_one_age_between_22_and_30',
        'premium_for_couple_with_one_age_between_31_and_40',
        'premium_for_couple_with_one_age_between_41_and_50',
        'premium_for_couple_with_one_age_between_51_and_60',
        'premium_for_couple_with_one_child_with_one_adult_age_between_19_and_21',
        'premium_for_couple_with_one_child_with_one_adult_age_between_22_and_30',
        'premium_for_couple_with_one_child_with_one_adult_age_between_31_and_40',
        'premium_for_couple_with_one_child_with_one_adult_age_between_41_and_50',
        'premium_for_couple_with_two_children_with_one_adult_age_between_19_and_21',
        'premium_for_couple_with_two_children_with_one_adult_age_between_22_and_30',
        'premium_for_couple_with_two_children_with_one_adult_age_between_31_and_40',
        'premium_for_couple_with_two_children_with_one_adult_age_between_41_and_50',
        'premium_for_couple_with_three_children_with_one_adult_age_between_19_and_21',
        'premium_for_couple_with_three_children_with_one_adult_age_between_22_and_30',
        'premium_for_couple_with_three_children_with_one_adult_age_between_31_and_40',
        'premium_for_couple_with_three_children_with_one_adult_age_between_41_and_50',
        'premium_for_single_parent_with_one_child_with_parent_age_between_19_and_21',
        'premium_for_single_parent_with_one_child_with_parent_age_between_22_and_30',
        'premium_for_single_parent_with_one_child_with_parent_age_between_31_and_40',
        'premium_for_single_parent_with_one_child_with_parent_age_between_41_and_50',
        'premium_for_single_parent_with_two_children_with_parent_age_between_19_and_21',
        'premium_for_single_parent_with_two_children_with_parent_age_between_22_and_30',
        'premium_for_single_parent_with_two_children_with_parent_age_between_31_and_40',
        'premium_for_single_parent_with_two_children_with_parent_age_between_41_and_50',
        'premium_for_single_parent_with_three_children_with_parent_age_between_19_and_21',
        'premium_for_single_parent_with_three_children_with_parent_age_between_22_and_30',
        'premium_for_single_parent_with_three_children_with_parent_age_between_31_and_40',
        'premium_for_single_parent_with_three_children_with_parent_age_between_41_and_50'
]

    insurance_plan_deductible_rates_columns = [
        'year',
        'state_code_for_insurance_issuer',
        'county_name_for_insurance_issuer',
        'insurance_plan_id',
        'deductible_for_medical_services_offered_to_individual',
        'deductible_for_prescription_drugs_offered_to_individual',
        'deductible_for_medical_services_offered_to_entire_family',
        'deductible_for_prescription_drugs_offered_to_entire_family',
        'deductible_for_medical_services_offered_per_person_in_family',
        'deductible_for_prescription_drugs_offered_per_person_in_family',
        'maximum_out_of_pocket_charges_for_medical_services_offered_to_individual',
        'maximum_out_of_pocket_charges_for_prescription_drugs_offered_to_individual',
        'maximum_out_of_pocket_charges_for_medical_services_offered_to_entire_family',
        'maximum_out_of_pocket_charges_for_prescription_drugs_offered_to_entire_family',
        'maximum_out_of_pocket_charges_for_medical_services_offered_per_person_in_family',
        'maximum_out_of_pocket_charges_for_prescription_drugs_offered_per_person_in_family',
        'copay_or_coinsurance_per_visit_to_primary_care_physician',
        'copay_or_coinsurance_per_visit_to_specialist',
        'copay_or_coinsurance_for_emergency_room_service',
        'copay_or_coinsurance_for_inpatient_facility_service',
        'copay_or_coinsurance_for_inpatient_physician_service',
        'copay_or_coinsurance_per_prescription_having_generic_drugs',
        'copay_or_coinsurance_per_prescription_having_preferred_brand_drugs',
        'copay_or_coinsurance_per_prescription_having_non_preferred_brand_drugs',
        'copay_or_coinsurance_per_prescription_having_specialty_drugs'
    ]

    insurance_plan_details = insurance_plans[insurance_plan_details_columns]
    insurance_plan_premium_rates = insurance_plans[insurance_plan_premium_rates_columns]
    insurance_plan_deductible_rates = insurance_plans[insurance_plan_deductible_rates_columns]


    #Saving the csv file locally in FILECACHE
    a = f"InsurancePlanDetails_{insurance_plan_year}.csv"
    b = f"InsurancePlanPremiumRates_{insurance_plan_year}.csv"
    c = f"InsurancePlanDeductibleRates_{insurance_plan_year}.csv"
    d = saveDFToCSV(csv_file_name=a, df=insurance_plan_details)
    e = saveDFToCSV(csv_file_name=b, df=insurance_plan_premium_rates)
    f = saveDFToCSV(csv_file_name=c, df=insurance_plan_deductible_rates)


    #Set XCOMs for GCS Uploading tasks
    gcs_prefix = 'insuranceplans'

    context['ti'].xcom_push(key='local_insurance_plan_details', value=d)
    context['ti'].xcom_push(key='bo_insurance_plan_details', value=f"{gcs_prefix}/{a}")
    context['ti'].xcom_push(key='local_insurance_plan_premium_rates', value=e)
    context['ti'].xcom_push(key='bo_insurance_plan_premium_rates', value=f"{gcs_prefix}/{b}")
    context['ti'].xcom_push(key='local_insurance_plan_deductible_rates', value=f)
    context['ti'].xcom_push(key='bo_insurance_plan_deductible_rates', value=f"{gcs_prefix}/{c}")

    context['ti'].xcom_push(key='insurance_plan_details_csvfilename', value=a)
    context['ti'].xcom_push(key='insurance_plan_premium_rates_csvfilename', value=b)
    context['ti'].xcom_push(key='insurance_plan_deductible_rates_csvfilename', value=c)



with DAG(
    dag_id="load_insurance_plans",
    start_date=days_ago(1),
    schedule_interval=None,
    params={
    "insurance_plan_year": Param(2024, type="number"),
    "insurance_plan_dataset_url": Param("https://data.healthcare.gov/datafile/py2024/individual_market_medical.zip", type="string")
    },
) as dag:

    downloadInsurancePlansExcelFile = PythonOperator(
        task_id = 'downloadInsurancePlansExcelFile',
        python_callable=task_downloadInsurancePlansExcelFile,
        provide_context=True
    )

    cleanAndConvertToCSVFile = PythonOperator(
        task_id = 'cleanAndConvertToCSVFile',
        python_callable=task_cleanAndConvertToCSVFile,
    )

    uploadInsurancePlanDetails_GCS = LocalFilesystemToGCSOperator(
        task_id = 'uploadInsurancePlanDetails_GCS',
        src= "{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='local_insurance_plan_details') }}",
        dst= "{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='bo_insurance_plan_details') }}",
        bucket= GCS_BUCKET,
        mime_type="application/octet-stream",
        gcp_conn_id="google_cloud_default",
    )

    uploadInsurancePlanPremiumRates_GCS = LocalFilesystemToGCSOperator(
        task_id = 'uploadInsurancePlanPremiumRates_GCS',
        src= "{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='local_insurance_plan_premium_rates') }}",
        dst= "{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='bo_insurance_plan_premium_rates') }}",
        bucket= GCS_BUCKET,
        mime_type="application/octet-stream",
        gcp_conn_id="google_cloud_default",
    )

    uploadInsurancePlanDeductibleRates_GCS = LocalFilesystemToGCSOperator(
        task_id = 'uploadInsurancePlanDeductibleRates_GCS',
        src= "{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='local_insurance_plan_deductible_rates') }}",
        dst= "{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='bo_insurance_plan_deductible_rates') }}",
        bucket= GCS_BUCKET,
        mime_type="application/octet-stream",
        gcp_conn_id="google_cloud_default",
    )

    syncDataWithSFTable_insurance_plan_details = SnowflakeOperator(
        task_id='syncDataWithSFTable_insurance_plan_details',
        sql="CALL load_insurance_plan_details('{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='insurance_plan_details_csvfilename') }}');",  
        snowflake_conn_id='snowflake_default',  
        autocommit=True,
    )

    syncDataWithSFTable_insurance_plan_premium_rates = SnowflakeOperator(
        task_id='syncDataWithSFTable_insurance_plan_premium_rates',
        sql="CALL load_insurance_plan_premium_rates('{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='insurance_plan_premium_rates_csvfilename') }}');",  
        snowflake_conn_id='snowflake_default',  
        autocommit=True,
    )

    syncDataWithSFTable_insurance_plan_deductible_rates = SnowflakeOperator(
        task_id='syncDataWithSFTable_insurance_plan_deductible_rates',
        sql="CALL load_insurance_plan_deductible_rates('{{ ti.xcom_pull(task_ids='cleanAndConvertToCSVFile', key='insurance_plan_deductible_rates_csvfilename') }}');",  
        snowflake_conn_id='snowflake_default',  
        autocommit=True,
    )

    downloadInsurancePlansExcelFile >> cleanAndConvertToCSVFile >> [uploadInsurancePlanDetails_GCS, uploadInsurancePlanPremiumRates_GCS, uploadInsurancePlanDeductibleRates_GCS]
    uploadInsurancePlanDetails_GCS >> syncDataWithSFTable_insurance_plan_details
    uploadInsurancePlanPremiumRates_GCS >> syncDataWithSFTable_insurance_plan_premium_rates
    uploadInsurancePlanDeductibleRates_GCS >> syncDataWithSFTable_insurance_plan_deductible_rates