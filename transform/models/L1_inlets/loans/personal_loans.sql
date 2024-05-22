{# {{ config(
    materialized = 'dynamic_table',
    snowflake_warehouse = 'wh_transforming',
    target_lag = 'downstream',
    persist_docs={"relation": false},
) }} #}

with raw_source as (

    select *
    from {{ source('LOANS', 'PERSONAL_LOANS') }}

),

final as (

    select
        "TOTAL_ACC"::float as total_acc,
        "ANNUAL_INC"::float as annual_inc,
        "EMP_LENGTH"::varchar as emp_length,
        "DESC"::varchar as desc,
        "TOTAL_PYMNT"::float as total_pymnt,
        "LAST_PYMNT_D"::varchar as last_pymnt_d,
        "ADDR_STATE"::varchar as addr_state,
        "NEXT_PYMNT_D"::varchar as next_pymnt_d,
        "EMP_TITLE"::varchar as emp_title,
        "COLLECTION_RECOVERY_FEE"::float as collection_recovery_fee,
        "MTHS_SINCE_LAST_MAJOR_DEROG"::float as mths_since_last_major_derog,
        "INQ_LAST_6MTHS"::float as inq_last_6mths,
        "SUB_GRADE"::varchar as sub_grade,
        "FUNDED_AMNT_INV"::float as funded_amnt_inv,
        "DELINQ_2YRS"::float as delinq_2yrs,
        "LOAN_ID"::varchar as loan_id,
        "FUNDED_AMNT"::float as funded_amnt,
        "VERIFICATION_STATUS"::varchar as verification_status,
        "DTI"::float as dti,
        "TOTAL_REC_PRNCP"::float as total_rec_prncp,
        "GRADE"::varchar as grade,
        "HOME_OWNERSHIP"::varchar as home_ownership,
        "ISSUE_D"::varchar as issue_d,
        "MTHS_SINCE_LAST_DELINQ"::float as mths_since_last_delinq,
        "OUT_PRNCP"::float as out_prncp,
        "PUB_REC"::float as pub_rec,
        "INT_RATE"::float as int_rate,
        "ZIP_CODE"::varchar as zip_code,
        "OPEN_ACC"::float as open_acc,
        "TERM"::varchar as term,
        "PYMNT_PLAN"::varchar as pymnt_plan,
        "URL"::varchar as url,
        "REVOL_BAL"::float as revol_bal,
        "RECOVERIES"::float as recoveries,
        "LAST_PYMNT_AMNT"::float as last_pymnt_amnt,
        "LOAN_AMNT"::float as loan_amnt,
        "PURPOSE"::varchar as purpose,
        "INITIAL_LIST_STATUS"::varchar as initial_list_status,
        "TOTAL_REC_INT"::float as total_rec_int,
        "TOTAL_PYMNT_INV"::float as total_pymnt_inv,
        "MTHS_SINCE_LAST_RECORD"::float as mths_since_last_record,
        "LAST_CREDIT_PULL_D"::varchar as last_credit_pull_d,
        "TOTAL_REC_LATE_FEE"::float as total_rec_late_fee,
        "MEMBER_ID"::float as member_id,
        "POLICY_CODE"::float as policy_code,
        "TITLE"::varchar as title,
        "LOAN_STATUS"::varchar as loan_status,
        "INSTALLMENT"::float as installment,
        "EARLIEST_CR_LINE"::varchar as earliest_cr_line,
        "REVOL_UTIL"::varchar as revol_util,
        "OUT_PRNCP_INV"::float as out_prncp_inv,
        "COLLECTIONS_12_MTHS_EX_MED"::float as collections_12_mths_ex_med

    from raw_source

)

select * from final
