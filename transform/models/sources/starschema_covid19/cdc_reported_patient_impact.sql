with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'cdc_reported_patient_impact') }}

),

final as (

    select
        "STATE" as state,
        "CRITICAL_STAFFING_SHORTAGE_TODAY_YES" as critical_staffing_shortage_today_yes,
        "CRITICAL_STAFFING_SHORTAGE_TODAY_NO" as critical_staffing_shortage_today_no,
        "CRITICAL_STAFFING_SHORTAGE_TODAY_NOT_REPORTED" as critical_staffing_shortage_today_not_reported,
        "CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_YES" as critical_staffing_shortage_anticipated_within_week_yes,
        "CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_NO" as critical_staffing_shortage_anticipated_within_week_no,
        "CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_NOT_REPORTED" as critical_staffing_shortage_anticipated_within_week_not_reported,
        "HOSPITAL_ONSET_COVID" as hospital_onset_covid,
        "HOSPITAL_ONSET_COVID_COVERAGE" as hospital_onset_covid_coverage,
        "INPATIENT_BEDS" as inpatient_beds,
        "INPATIENT_BEDS_COVERAGE" as inpatient_beds_coverage,
        "INPATIENT_BEDS_USED" as inpatient_beds_used,
        "INPATIENT_BEDS_USED_COVERAGE" as inpatient_beds_used_coverage,
        "INPATIENT_BEDS_USED_COVID" as inpatient_beds_used_covid,
        "INPATIENT_BEDS_USED_COVID_COVERAGE" as inpatient_beds_used_covid_coverage,
        "PREVIOUS_DAY_ADMISSION_ADULT_COVID_CONFIRMED" as previous_day_admission_adult_covid_confirmed,
        "PREVIOUS_DAY_ADMISSION_ADULT_COVID_CONFIRMED_COVERAGE" as previous_day_admission_adult_covid_confirmed_coverage,
        "PREVIOUS_DAY_ADMISSION_ADULT_COVID_SUSPECTED" as previous_day_admission_adult_covid_suspected,
        "PREVIOUS_DAY_ADMISSION_ADULT_COVID_SUSPECTED_COVERAGE" as previous_day_admission_adult_covid_suspected_coverage,
        "PREVIOUS_DAY_ADMISSION_PEDIATRIC_COVID_CONFIRMED" as previous_day_admission_pediatric_covid_confirmed,
        "PREVIOUS_DAY_ADMISSION_PEDIATRIC_COVID_CONFIRMED_COVERAGE" as previous_day_admission_pediatric_covid_confirmed_coverage,
        "PREVIOUS_DAY_ADMISSION_PEDIATRIC_COVID_SUSPECTED" as previous_day_admission_pediatric_covid_suspected,
        "PREVIOUS_DAY_ADMISSION_PEDIATRIC_COVID_SUSPECTED_COVERAGE" as previous_day_admission_pediatric_covid_suspected_coverage,
        "STAFFED_ADULT_ICU_BED_OCCUPANCY" as staffed_adult_icu_bed_occupancy,
        "STAFFED_ADULT_ICU_BED_OCCUPANCY_COVERAGE" as staffed_adult_icu_bed_occupancy_coverage,
        "STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_AND_SUSPECTED_COVID" as staffed_icu_adult_patients_confirmed_and_suspected_covid,
        "STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_AND_SUSPECTED_COVID_COVERAGE" as staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage,
        "STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_COVID" as staffed_icu_adult_patients_confirmed_covid,
        "STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_COVID_COVERAGE" as staffed_icu_adult_patients_confirmed_covid_coverage,
        "TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID" as total_adult_patients_hospitalized_confirmed_and_suspected_covid,
        "TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID_COVERAGE" as total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage,
        "TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_COVID" as total_adult_patients_hospitalized_confirmed_covid,
        "TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_COVID_COVERAGE" as total_adult_patients_hospitalized_confirmed_covid_coverage,
        "TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID" as total_pediatric_patients_hospitalized_confirmed_and_suspected_covid,
        "TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID_COVERAGE" as total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage,
        "TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_COVID" as total_pediatric_patients_hospitalized_confirmed_covid,
        "TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_COVID_COVERAGE" as total_pediatric_patients_hospitalized_confirmed_covid_coverage,
        "TOTAL_STAFFED_ADULT_ICU_BEDS" as total_staffed_adult_icu_beds,
        "TOTAL_STAFFED_ADULT_ICU_BEDS_COVERAGE" as total_staffed_adult_icu_beds_coverage,
        "INPATIENT_BEDS_UTILIZATION" as inpatient_beds_utilization,
        "INPATIENT_BEDS_UTILIZATION_COVERAGE" as inpatient_beds_utilization_coverage,
        "INPATIENT_BEDS_UTILIZATION_NUMERATOR" as inpatient_beds_utilization_numerator,
        "INPATIENT_BEDS_UTILIZATION_DENOMINATOR" as inpatient_beds_utilization_denominator,
        "PERCENT_OF_INPATIENTS_WITH_COVID" as percent_of_inpatients_with_covid,
        "PERCENT_OF_INPATIENTS_WITH_COVID_COVERAGE" as percent_of_inpatients_with_covid_coverage,
        "PERCENT_OF_INPATIENTS_WITH_COVID_NUMERATOR" as percent_of_inpatients_with_covid_numerator,
        "PERCENT_OF_INPATIENTS_WITH_COVID_DENOMINATOR" as percent_of_inpatients_with_covid_denominator,
        "INPATIENT_BED_COVID_UTILIZATION" as inpatient_bed_covid_utilization,
        "INPATIENT_BED_COVID_UTILIZATION_COVERAGE" as inpatient_bed_covid_utilization_coverage,
        "INPATIENT_BED_COVID_UTILIZATION_NUMERATOR" as inpatient_bed_covid_utilization_numerator,
        "INPATIENT_BED_COVID_UTILIZATION_DENOMINATOR" as inpatient_bed_covid_utilization_denominator,
        "ADULT_ICU_BED_COVID_UTILIZATION" as adult_icu_bed_covid_utilization,
        "ADULT_ICU_BED_COVID_UTILIZATION_COVERAGE" as adult_icu_bed_covid_utilization_coverage,
        "ADULT_ICU_BED_COVID_UTILIZATION_NUMERATOR" as adult_icu_bed_covid_utilization_numerator,
        "ADULT_ICU_BED_COVID_UTILIZATION_DENOMINATOR" as adult_icu_bed_covid_utilization_denominator,
        "ADULT_ICU_BED_UTILIZATION" as adult_icu_bed_utilization,
        "ADULT_ICU_BED_UTILIZATION_COVERAGE" as adult_icu_bed_utilization_coverage,
        "ADULT_ICU_BED_UTILIZATION_NUMERATOR" as adult_icu_bed_utilization_numerator,
        "ADULT_ICU_BED_UTILIZATION_DENOMINATOR" as adult_icu_bed_utilization_denominator,
        "DATE" as date,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
