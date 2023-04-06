with raw_source as (

    select *
    from {{ source('LOANS', '_AIRBYTE_RAW_PERSONAL_LOANS') }}
limit 3
),

final as (

    select
        _airbyte_data:"ADDR_STATE"::varchar as addr_state,
        _airbyte_data:"ANNUAL_INC"::numeric as annual_inc,
        _airbyte_data:"COLLECTIONS_12_MTHS_EX_MED"::varchar as collections_12_mths_ex_med,
        _airbyte_data:"COLLECTION_RECOVERY_FEE"::varchar as collection_recovery_fee,
        _airbyte_data:"DELINQ_2YRS"::varchar as delinq_2yrs,
        _airbyte_data:"DESC"::varchar as desc,
        _airbyte_data:"DTI"::varchar as dti,
        _airbyte_data:"EARLIEST_CR_LINE"::varchar as earliest_cr_line,
        _airbyte_data:"EMP_LENGTH"::varchar as emp_length,
        _airbyte_data:"EMP_TITLE"::varchar as emp_title,
        _airbyte_data:"FUNDED_AMNT"::varchar as funded_amnt,
        _airbyte_data:"FUNDED_AMNT_INV"::varchar as funded_amnt_inv,
        _airbyte_data:"GRADE"::varchar as grade,
        _airbyte_data:"HOME_OWNERSHIP"::varchar as home_ownership,
        _airbyte_data:"INITIAL_LIST_STATUS"::varchar as initial_list_status,
        _airbyte_data:"INQ_LAST_6MTHS"::varchar as inq_last_6mths,
        _airbyte_data:"INSTALLMENT"::varchar as installment,
        _airbyte_data:"INT_RATE"::float as int_rate,
        _airbyte_data:"ISSUE_D"::varchar as issue_d,
        _airbyte_data:"LAST_CREDIT_PULL_D"::varchar as last_credit_pull_d,
        _airbyte_data:"LAST_PYMNT_AMNT"::varchar as last_pymnt_amnt,
        _airbyte_data:"LAST_PYMNT_D"::varchar as last_pymnt_d,
        _airbyte_data:"LOAN_AMNT"::varchar as loan_amnt,
        _airbyte_data:"LOAN_ID"::varchar as loan_id,
        _airbyte_data:"LOAN_STATUS"::varchar as loan_status,
        _airbyte_data:"MEMBER_ID"::varchar as member_id,
        _airbyte_data:"MTHS_SINCE_LAST_DELINQ"::varchar as mths_since_last_delinq,
        _airbyte_data:"MTHS_SINCE_LAST_MAJOR_DEROG"::varchar as mths_since_last_major_derog,
        _airbyte_data:"MTHS_SINCE_LAST_RECORD"::varchar as mths_since_last_record,
        _airbyte_data:"NEXT_PYMNT_D"::varchar as next_pymnt_d,
        _airbyte_data:"OPEN_ACC"::varchar as open_acc,
        _airbyte_data:"OUT_PRNCP"::varchar as out_prncp,
        _airbyte_data:"OUT_PRNCP_INV"::varchar as out_prncp_inv,
        _airbyte_data:"POLICY_CODE"::varchar as policy_code,
        _airbyte_data:"PUB_REC"::varchar as pub_rec,
        _airbyte_data:"PURPOSE"::varchar as purpose,
        _airbyte_data:"PYMNT_PLAN"::varchar as pymnt_plan,
        _airbyte_data:"RECOVERIES"::varchar as recoveries,
        _airbyte_data:"REVOL_BAL"::varchar as revol_bal,
        _airbyte_data:"REVOL_UTIL"::varchar as revol_util,
        _airbyte_data:"SUB_GRADE"::varchar as sub_grade,
        _airbyte_data:"TERM"::varchar as term,
        _airbyte_data:"TITLE"::varchar as title,
        _airbyte_data:"TOTAL_ACC"::varchar as total_acc,
        _airbyte_data:"TOTAL_PYMNT"::varchar as total_pymnt,
        _airbyte_data:"TOTAL_PYMNT_INV"::varchar as total_pymnt_inv,
        _airbyte_data:"TOTAL_REC_INT"::varchar as total_rec_int,
        _airbyte_data:"TOTAL_REC_LATE_FEE"::varchar as total_rec_late_fee,
        _airbyte_data:"TOTAL_REC_PRNCP"::varchar as total_rec_prncp,
        _airbyte_data:"URL"::varchar as url,
        _airbyte_data:"VERIFICATION_STATUS"::varchar as verification_status,
        _airbyte_data:"ZIP_CODE"::varchar as zip_code,
        "_AIRBYTE_AB_ID"::varchar as airbyte_ab_id,
        "_AIRBYTE_DATA"::variant as airbyte_data,
        "_AIRBYTE_EMITTED_AT"::timestamp_tz as airbyte_emitted_at

    from raw_source

)

select * from final
