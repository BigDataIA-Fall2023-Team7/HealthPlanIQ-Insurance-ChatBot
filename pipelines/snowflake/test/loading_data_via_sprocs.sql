CALL load_insurance_plan_details('InsurancePlanDetails_2024.csv');
CALL load_insurance_plan_details('InsurancePlanDetails_2023.csv');
CALL load_insurance_plan_details('InsurancePlanDetails_2022.csv');

CALL load_insurance_plan_premium_rates('InsurancePlanPremiumRates_2024.csv');
CALL load_insurance_plan_premium_rates('InsurancePlanPremiumRates_2023.csv');
CALL load_insurance_plan_premium_rates('InsurancePlanPremiumRates_2022.csv');

CALL load_insurance_plan_deductible_rates('InsurancePlanDeductibleRates_2024.csv');
CALL load_insurance_plan_deductible_rates('InsurancePlanDeductibleRates_2023.csv');
CALL load_insurance_plan_deductible_rates('InsurancePlanDeductibleRates_2022.csv');








CALL load_insurance_plan_benefits('InsurancePlanBenefitsCoverage_2024.csv');
CALL load_insurance_plan_benefits('InsurancePlanBenefitsCoverage_2023.csv');
CALL load_insurance_plan_benefits('InsurancePlanBenefitsCoverage_2022.csv');








CALL load_stg_insurance_plan_drug_coverage('AK_21989_bef84374df4ac78e369d225248d4d7e35348c2ec548e4bbb43ae1d1842807ca5.csv');
CALL merge_stg_to_insurance_plan_drug_coverage();









CALL load_insurance_plan_quality_ratings('InsurancePlanQualityRatings_2024.csv');
CALL load_insurance_plan_quality_ratings('InsurancePlanQualityRatings_2023.csv');
CALL load_insurance_plan_quality_ratings('InsurancePlanQualityRatings_2022.csv');






