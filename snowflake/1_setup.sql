-- Use the USA_QHP.PUBLIC

-- show integrations;

-- Integration object to authenticate GCS bucket access where Airflow processed CSV files will be staged
-- We are using GCS bucket external stage area for Snowflake

create database USA_QHP;

use schema public;

CREATE OR REPLACE STORAGE INTEGRATION GCP_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ("gcs://usa_qhp/");

-- DESC STORAGE INTEGRATION GCP_INT;

-- Stage object (gcs_stage_usa_qhp_insuranceplans)
create or replace stage gcs_stage_usa_qhp_insuranceplans
  url = 'gcs://usa_qhp/insuranceplans/'
  storage_integration = gcp_int;

-- Stage object (gcs_stage_usa_qhp_insuranceplan_benefits)
create or replace stage gcs_stage_usa_qhp_insuranceplan_benefits
  url = 'gcs://usa_qhp/insuranceplan_benefits/'
  storage_integration = gcp_int;

-- Stage object (gcs_stage_usa_qhp_insuranceplan_drug_coverage)
create or replace stage gcs_stage_usa_qhp_insuranceplan_drug_coverage
  url = 'gcs://usa_qhp/insuranceplan_drug_coverage/'
  storage_integration = gcp_int;


-- Stage object (gcs_stage_usa_qhp_insuranceplan_drug_coverage)
create or replace stage gcs_stage_usa_qhp_insuranceplan_quality_ratings
url = 'gcs://usa_qhp/insuranceplan_quality_ratings/'
storage_integration = gcp_int;

-- List files in bucket :  usa_qhp
-- list @gcs_stage_usa_qhp_insuranceplans;
-- list @gcs_stage_usa_qhp_insuranceplan_benefits;
-- list @gcs_stage_usa_qhp_insuranceplan_drug_coverage;
-- list @gcs_stage_usa_qhp_insuranceplan_quality_ratings;


-- CSV File Format for snowflake to map gcs bucket csv files to snowflake tables
CREATE OR REPLACE FILE FORMAT csv_format_usa_qhp
  TYPE = 'CSV'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE = 'NONE'
  ESCAPE_UNENCLOSED_FIELD = NONE
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('')
  EMPTY_FIELD_AS_NULL = TRUE


-- TABLE CREATIONS

-- t_year_wise_insurance_plan_details

CREATE OR REPLACE TABLE USA_QHP.PUBLIC.t_year_wise_insurance_plan_details (
    year INT,
    state_code_for_insurance_issuer STRING,
    county_name_for_insurance_issuer STRING,
    insurance_plan_id STRING,
    insurance_plan_name STRING,
    insurance_plan_type STRING,
    insurance_plan_level STRING,
    insurance_issuer_id INT,
    insurance_issuer_name STRING,
    rating_area_for_insurance_plan STRING,
    customer_service_phone_number STRING,
    tollfree_customer_service_phone_number STRING,
    tty_customer_service_phone_number STRING,
    url_for_insurance_issuer_network STRING,
    url_for_insurance_plan_brochure STRING,
    url_for_summary_of_insurance_benefits STRING,
    url_for_drugs_supported_by_insurance_plan STRING,
    is_adult_dental_covered BOOLEAN,
    is_child_dental_covered BOOLEAN,

    CONSTRAINT pk_t_year_wise_insurance_plan_details 
    PRIMARY KEY (year ,state_code_for_insurance_issuer, county_name_for_insurance_issuer,insurance_plan_id)
);


-- t_year_wise_insurance_plan_premium_rates
CREATE OR REPLACE TABLE USA_QHP.PUBLIC.t_year_wise_insurance_plan_premium_rates (
    year INT,
    state_code_for_insurance_issuer STRING,
    county_name_for_insurance_issuer STRING,
    insurance_plan_id STRING,
    premium_for_child_with_age_between_0_and_14 FLOAT,
    premium_for_child_with_age_between_15_and_18 FLOAT,
    premium_for_adult_with_age_between_19_and_21 FLOAT,
    premium_for_adult_with_age_between_22_and_27 FLOAT,
    premium_for_adult_with_age_between_28_and_30 FLOAT,
    premium_for_adult_with_age_between_31_and_40 FLOAT,
    premium_for_adult_with_age_between_41_and_50 FLOAT,
    premium_for_adult_with_age_between_51_and_60 FLOAT,
    premium_for_couple_with_one_age_between_19_and_21 FLOAT,
    premium_for_couple_with_one_age_between_22_and_30 FLOAT,
    premium_for_couple_with_one_age_between_31_and_40 FLOAT,
    premium_for_couple_with_one_age_between_41_and_50 FLOAT,
    premium_for_couple_with_one_age_between_51_and_60 FLOAT,
    premium_for_couple_with_one_child_with_one_adult_age_between_19_and_21 FLOAT,
    premium_for_couple_with_one_child_with_one_adult_age_between_22_and_30 FLOAT,
    premium_for_couple_with_one_child_with_one_adult_age_between_31_and_40 FLOAT,
    premium_for_couple_with_one_child_with_one_adult_age_between_41_and_50 FLOAT,
    premium_for_couple_with_two_children_with_one_adult_age_between_19_and_21 FLOAT,
    premium_for_couple_with_two_children_with_one_adult_age_between_22_and_30 FLOAT,
    premium_for_couple_with_two_children_with_one_adult_age_between_31_and_40 FLOAT,
    premium_for_couple_with_two_children_with_one_adult_age_between_41_and_50 FLOAT,
    premium_for_couple_with_three_children_with_one_adult_age_between_19_and_21 FLOAT,
    premium_for_couple_with_three_children_with_one_adult_age_between_22_and_30 FLOAT,
    premium_for_couple_with_three_children_with_one_adult_age_between_31_and_40 FLOAT,
    premium_for_couple_with_three_children_with_one_adult_age_between_41_and_50 FLOAT,
    premium_for_single_parent_with_one_child_with_parent_age_between_19_and_21 FLOAT,
    premium_for_single_parent_with_one_child_with_parent_age_between_22_and_30 FLOAT,
    premium_for_single_parent_with_one_child_with_parent_age_between_31_and_40 FLOAT,
    premium_for_single_parent_with_one_child_with_parent_age_between_41_and_50 FLOAT,
    premium_for_single_parent_with_two_children_with_parent_age_between_19_and_21 FLOAT,
    premium_for_single_parent_with_two_children_with_parent_age_between_22_and_30 FLOAT,
    premium_for_single_parent_with_two_children_with_parent_age_between_31_and_40 FLOAT,
    premium_for_single_parent_with_two_children_with_parent_age_between_41_and_50 FLOAT,
    premium_for_single_parent_with_three_children_with_parent_age_between_19_and_21 FLOAT,
    premium_for_single_parent_with_three_children_with_parent_age_between_22_and_30 FLOAT,
    premium_for_single_parent_with_three_children_with_parent_age_between_31_and_40 FLOAT,
    premium_for_single_parent_with_three_children_with_parent_age_between_41_and_50 FLOAT,

    CONSTRAINT pk_t_year_wise_insurance_plan_premium_rates
    PRIMARY KEY (year ,state_code_for_insurance_issuer, county_name_for_insurance_issuer,insurance_plan_id),

    CONSTRAINT fk_t_year_wise_insurance_plan_premium_rates
    FOREIGN KEY (year ,state_code_for_insurance_issuer, county_name_for_insurance_issuer,insurance_plan_id)
    REFERENCES t_year_wise_insurance_plan_details(year,state_code_for_insurance_issuer,county_name_for_insurance_issuer,insurance_plan_id)
);

-- t_year_wise_insurance_plan_deductible_rates
CREATE OR REPLACE TABLE USA_QHP.PUBLIC.t_year_wise_insurance_plan_deductible_rates (
    year INT,
    state_code_for_insurance_issuer STRING,
    county_name_for_insurance_issuer STRING,
    insurance_plan_id STRING,
    deductible_for_medical_services_offered_to_individual STRING,
    deductible_for_prescription_drugs_offered_to_individual STRING,
    deductible_for_medical_services_offered_to_entire_family STRING,
    deductible_for_prescription_drugs_offered_to_entire_family STRING,
    deductible_for_medical_services_offered_per_person_in_family STRING,
    deductible_for_prescription_drugs_offered_per_person_in_family STRING,
    maximum_out_of_pocket_charges_for_medical_services_offered_to_individual STRING,
    maximum_out_of_pocket_charges_for_prescription_drugs_offered_to_individual STRING,
    maximum_out_of_pocket_charges_for_medical_services_offered_to_entire_family STRING,
    maximum_out_of_pocket_charges_for_prescription_drugs_offered_to_entire_family STRING,
    maximum_out_of_pocket_charges_for_medical_services_offered_per_person_in_family STRING,
    maximum_out_of_pocket_charges_for_prescription_drugs_offered_per_person_in_family STRING,
    copay_or_coinsurance_per_visit_to_primary_care_physician STRING,
    copay_or_coinsurance_per_visit_to_specialist STRING,
    copay_or_coinsurance_for_emergency_room_service STRING,
    copay_or_coinsurance_for_inpatient_facility_service STRING,
    copay_or_coinsurance_for_inpatient_physician_service STRING,
    copay_or_coinsurance_per_prescription_having_generic_drugs STRING,
    copay_or_coinsurance_per_prescription_having_preferred_brand_drugs STRING,
    copay_or_coinsurance_per_prescription_having_non_preferred_brand_drugs STRING,
    copay_or_coinsurance_per_prescription_having_specialty_drugs STRING,    

    CONSTRAINT pk_t_year_wise_insurance_plan_deductible_rates
    PRIMARY KEY (year ,state_code_for_insurance_issuer, county_name_for_insurance_issuer,insurance_plan_id),

    CONSTRAINT fk_t_year_wise_insurance_plan_deductible_rates 
    FOREIGN KEY (year ,state_code_for_insurance_issuer, county_name_for_insurance_issuer,insurance_plan_id)
    REFERENCES t_year_wise_insurance_plan_details(year,state_code_for_insurance_issuer,county_name_for_insurance_issuer,insurance_plan_id)
);

-- t_year_wise_insurance_plan_benefits
CREATE OR REPLACE TABLE USA_QHP.PUBLIC.t_year_wise_insurance_plan_benefits (
  year INT,
  state_code_for_insurance_issuer STRING,
  insurance_plan_id STRING,
  insurance_plan_benefit_name STRING,
  is_insurance_plan_benefit_covered STRING,
  
  CONSTRAINT pk_t_year_wise_insurance_plan_benefits
  PRIMARY KEY (year ,state_code_for_insurance_issuer, insurance_plan_id, insurance_plan_benefit_name)
    
);

create schema stg;

use schema stg;

-- stg_t_year_wise_insurance_plan_drug_coverage
CREATE OR REPLACE TABLE USA_QHP.STG.stg_t_year_wise_insurance_plan_drug_coverage (
    year INT,
    insurance_plan_id STRING, 
    rxnorm_drug_id STRING, 
    insurance_plan_covered_drug_name STRING, 
    insurance_plan_covered_drug_tier STRING
);

use schema public;

-- t_year_wise_insurance_plan_drug_coverage
CREATE OR REPLACE TABLE USA_QHP.PUBLIC.t_year_wise_insurance_plan_drug_coverage (
    year INT,
    insurance_plan_id STRING, 
    rxnorm_drug_id STRING, 
    insurance_plan_covered_drug_name STRING, 
    insurance_plan_covered_drug_tier STRING, 
    
    CONSTRAINT pk_t_year_wise_insurance_plan_drug_coverage
    PRIMARY KEY (year ,insurance_plan_id, rxnorm_drug_id)
);

-- t_year_wise_insurance_plan_quality_ratings
CREATE OR REPLACE TABLE USA_QHP.PUBLIC.t_year_wise_insurance_plan_quality_ratings (
    year INT,
    insurance_plan_id STRING,
    insurance_plan_overall_quality_rating INT,
    insurance_plan_quality_rating_for_medical_care INT,
    insurance_plan_quality_rating_for_member_experience INT,
    insurance_plan_quality_rating_for_plan_administration INT,

    CONSTRAINT pk_t_year_wise_insurance_plan_quality_ratings
    PRIMARY KEY (year ,insurance_plan_id)
);

-- show tables;

-- -------------------------------------
-- Snowflake Stored Procedures to load data from external GCS stage area to SF tables
-- This procedures will be triggered by Airflow for data syncing from GCS to SF
-- -------------------------------------


-- SPROC : load_insurance_plan_details(<GCS File Name>)
CREATE OR REPLACE PROCEDURE USA_QHP.PUBLIC.load_insurance_plan_details(
  file_name VARCHAR(255)
)
RETURNS VARCHAR(255)
language sql
AS
BEGIN
  EXECUTE IMMEDIATE
  'COPY INTO 
    ' ||'T_YEAR_WISE_INSURANCE_PLAN_DETAILS'|| '
  FROM 
    @gcs_stage_usa_qhp_insuranceplans
    FILES = (''' || TRIM(:file_name) || ''')
    FILE_FORMAT = (FORMAT_NAME = csv_format_usa_qhp)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = ABORT_STATEMENT;';
END;


-- SPROC : load_insurance_plan_premium_rates(<GCS File Name>)
CREATE OR REPLACE PROCEDURE USA_QHP.PUBLIC.load_insurance_plan_premium_rates(
  file_name VARCHAR(255)
)
RETURNS VARCHAR(255)
language sql
AS
BEGIN
  EXECUTE IMMEDIATE
  'COPY INTO 
    ' ||'t_year_wise_insurance_plan_premium_rates'|| '
  FROM 
    @gcs_stage_usa_qhp_insuranceplans
    FILES = (''' || TRIM(:file_name) || ''')
    FILE_FORMAT = (FORMAT_NAME = csv_format_usa_qhp)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = ABORT_STATEMENT;';
END;


-- SPROC : load_insurance_plan_deductible_rates(<GCS File Name>)

CREATE OR REPLACE PROCEDURE USA_QHP.PUBLIC.load_insurance_plan_deductible_rates(
  file_name VARCHAR(255)
)
RETURNS VARCHAR(255)
language sql
AS
BEGIN
  EXECUTE IMMEDIATE
  'COPY INTO 
    ' ||'t_year_wise_insurance_plan_deductible_rates'|| '
  FROM 
    @gcs_stage_usa_qhp_insuranceplans
    FILES = (''' || TRIM(:file_name) || ''')
    FILE_FORMAT = (FORMAT_NAME = csv_format_usa_qhp)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = ABORT_STATEMENT;';
END;

-- SPROC : load_insurance_plan_benefits(<GCS File Name>)

CREATE OR REPLACE PROCEDURE USA_QHP.PUBLIC.load_insurance_plan_benefits(
  file_name VARCHAR(255)
)
RETURNS VARCHAR(255)
language sql
AS
BEGIN
  EXECUTE IMMEDIATE
  'COPY INTO 
    ' || 't_year_wise_insurance_plan_benefits' || '
  FROM 
    @gcs_stage_usa_qhp_insuranceplan_benefits
    FILES = (''' || TRIM(:file_name) || ''')
    FILE_FORMAT = (FORMAT_NAME = csv_format_usa_qhp)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = ABORT_STATEMENT;';
END;


-- SPROC : load_stg_insurance_plan_drug_coverage(<GCS File Name>)
-- Sproc to load GCS data to drug coverage stage table
CREATE OR REPLACE PROCEDURE USA_QHP.PUBLIC.load_stg_insurance_plan_drug_coverage(
  file_name VARCHAR(255)
)
RETURNS VARCHAR(255)
language sql
AS
BEGIN
  EXECUTE IMMEDIATE
  'COPY INTO 
    ' || 'USA_QHP.STG.STG_T_YEAR_WISE_INSURANCE_PLAN_DRUG_COVERAGE' || '
  FROM 
    @gcs_stage_usa_qhp_insuranceplan_drug_coverage
    FILES = (''' || TRIM(:file_name) || ''')
    FILE_FORMAT = (FORMAT_NAME = csv_format_usa_qhp)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = ABORT_STATEMENT;';
END;


-- SPROC : merge_stg_to_insurance_plan_drug_coverage(<GCS File Name>)
-- Sproc to load unique drug coverage records to main drug coverage table

CREATE OR REPLACE PROCEDURE USA_QHP.PUBLIC.merge_stg_to_insurance_plan_drug_coverage()
RETURNS VARCHAR(255)
language sql
AS
BEGIN
  EXECUTE IMMEDIATE
  '
    MERGE INTO t_year_wise_insurance_plan_drug_coverage AS target
    USING (
        SELECT DISTINCT * FROM usa_qhp.stg.stg_t_year_wise_insurance_plan_drug_coverage
    ) AS source
    ON target.year = source.year
       AND target.insurance_plan_id = source.insurance_plan_id
       AND target.rxnorm_drug_id = source.rxnorm_drug_id
       AND target.insurance_plan_covered_drug_name = source.insurance_plan_covered_drug_name
       AND target.insurance_plan_covered_drug_tier = source.insurance_plan_covered_drug_tier
    WHEN NOT MATCHED THEN
      INSERT (year, insurance_plan_id, rxnorm_drug_id, insurance_plan_covered_drug_name, insurance_plan_covered_drug_tier)
      VALUES (source.year, source.insurance_plan_id, source.rxnorm_drug_id, source.insurance_plan_covered_drug_name, source.insurance_plan_covered_drug_tier);
  ';
END;


-- Views for Streamlit UI metadata
create or replace view USA_QHP.PUBLIC.STATE_COUNTY(
	STATE_CODE,
	COUNTY
) as 
SELECT DISTINCT state_code_for_insurance_issuer as STATE_CODE,
COUNTY_NAME_FOR_INSURANCE_ISSUER as COUNTY
FROM
USA_QHP.PUBLIC.T_YEAR_WISE_INSURANCE_PLAN_DETAILS
order by STATE_CODE, COUNTY;
