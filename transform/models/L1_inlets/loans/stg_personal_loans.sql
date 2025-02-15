{{ config(
    materialized = 'dynamic_table',
    snowflake_warehouse = 'wh_transforming_dynamic_tables',
    target_lag = 'downstream',
) }}

with raw_source as (

    select *
    from {{ source('LOANS', 'PERSONAL_LOANS') }}

),

final as (

    select
        "LOAN_ID"::varchar as loan_id,
        "MEMBER_ID"::number as member_id,
        "LOAN_AMNT"::number as loan_amnt,
        "FUNDED_AMNT"::number as funded_amnt,
        "FUNDED_AMNT_INV"::float as funded_amnt_inv,
        "TERM"::varchar as term,
        "INT_RATE"::float as int_rate,
        "INSTALLMENT"::float as installment,
        "GRADE"::varchar as grade,
        "SUB_GRADE"::varchar as sub_grade,
        "EMP_TITLE"::varchar as emp_title,
        "EMP_LENGTH"::varchar as emp_length,
        "HOME_OWNERSHIP"::varchar as home_ownership,
        "ANNUAL_INC"::float as annual_inc,
        "VERIFICATION_STATUS"::varchar as verification_status,
        "ISSUE_D"::varchar as issue_d,
        "LOAN_STATUS"::varchar as loan_status,
        "PYMNT_PLAN"::varchar as pymnt_plan,
        "URL"::varchar as url,
        "DESC"::varchar as desc,
        "PURPOSE"::varchar as purpose,
        "TITLE"::varchar as title,
        "ZIP_CODE"::varchar as zip_code,
        "ADDR_STATE"::varchar as addr_state,
        "DTI"::float as dti,
        "DELINQ_2_YRS"::float as delinq_2_yrs,
        "EARLIEST_CR_LINE"::varchar as earliest_cr_line,
        "INQ_LAST_6_MTHS"::float as inq_last_6_mths,
        "MTHS_SINCE_LAST_DELINQ"::float as mths_since_last_delinq,
        "MTHS_SINCE_LAST_RECORD"::float as mths_since_last_record,
        "OPEN_ACC"::float as open_acc,
        "PUB_REC"::float as pub_rec,
        "REVOL_BAL"::number as revol_bal,
        "REVOL_UTIL"::varchar as revol_util,
        "TOTAL_ACC"::float as total_acc,
        "INITIAL_LIST_STATUS"::varchar as initial_list_status,
        "OUT_PRNCP"::float as out_prncp,
        "OUT_PRNCP_INV"::float as out_prncp_inv,
        "TOTAL_PYMNT"::float as total_pymnt,
        "TOTAL_PYMNT_INV"::float as total_pymnt_inv,
        "TOTAL_REC_PRNCP"::float as total_rec_prncp,
        "TOTAL_REC_INT"::float as total_rec_int,
        "TOTAL_REC_LATE_FEE"::float as total_rec_late_fee,
        "RECOVERIES"::float as recoveries,
        "COLLECTION_RECOVERY_FEE"::float as collection_recovery_fee,
        "LAST_PYMNT_D"::varchar as last_pymnt_d,
        "LAST_PYMNT_AMNT"::float as last_pymnt_amnt,
        "NEXT_PYMNT_D"::varchar as next_pymnt_d,
        "LAST_CREDIT_PULL_D"::varchar as last_credit_pull_d,
        "COLLECTIONS_12_MTHS_EX_MED"::float as collections_12_mths_ex_med,
        "MTHS_SINCE_LAST_MAJOR_DEROG"::float as mths_since_last_major_derog,
        "POLICY_CODE"::number as policy_code

    from raw_source

)

select * from final
