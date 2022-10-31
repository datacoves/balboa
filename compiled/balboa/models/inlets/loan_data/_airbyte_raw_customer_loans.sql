with raw_source as (

    select *
    from RAW.datameer._airbyte_raw_customer_loans

),

final as (

    select
        _airbyte_data:"addr_state"::varchar as addr_state,
        _airbyte_data:"annual_inc"::varchar as annual_inc,
        _airbyte_data:"collection_recovery_fee"::varchar as collection_recovery_fee,
        _airbyte_data:"collections_12_mths_ex_med"::varchar as collections_12_mths_ex_med,
        _airbyte_data:"delinq_2yrs"::varchar as delinq_2yrs,
        _airbyte_data:"desc"::varchar as desc,
        _airbyte_data:"dti"::varchar as dti,
        _airbyte_data:"earliest_cr_line"::varchar as earliest_cr_line,
        _airbyte_data:"emp_length"::varchar as emp_length,
        _airbyte_data:"emp_title"::varchar as emp_title,
        _airbyte_data:"funded_amnt"::varchar as funded_amnt,
        _airbyte_data:"funded_amnt_inv"::varchar as funded_amnt_inv,
        _airbyte_data:"grade"::varchar as grade,
        _airbyte_data:"home_ownership"::varchar as home_ownership,
        _airbyte_data:"id"::varchar as id,
        _airbyte_data:"initial_list_status"::varchar as initial_list_status,
        _airbyte_data:"inq_last_6mths"::varchar as inq_last_6mths,
        _airbyte_data:"installment"::varchar as installment,
        _airbyte_data:"int_rate"::varchar as int_rate,
        _airbyte_data:"issue_d"::varchar as issue_d,
        _airbyte_data:"last_credit_pull_d"::varchar as last_credit_pull_d,
        _airbyte_data:"last_pymnt_amnt"::varchar as last_pymnt_amnt,
        _airbyte_data:"last_pymnt_d"::varchar as last_pymnt_d,
        _airbyte_data:"loan_amnt"::varchar as loan_amnt,
        _airbyte_data:"loan_status"::varchar as loan_status,
        _airbyte_data:"member_id"::varchar as member_id,
        _airbyte_data:"mths_since_last_delinq"::varchar as mths_since_last_delinq,
        _airbyte_data:"mths_since_last_major_derog"::varchar as mths_since_last_major_derog,
        _airbyte_data:"mths_since_last_record"::varchar as mths_since_last_record,
        _airbyte_data:"next_pymnt_d"::varchar as next_pymnt_d,
        _airbyte_data:"open_acc"::varchar as open_acc,
        _airbyte_data:"out_prncp"::varchar as out_prncp,
        _airbyte_data:"out_prncp_inv"::varchar as out_prncp_inv,
        _airbyte_data:"policy_code"::varchar as policy_code,
        _airbyte_data:"pub_rec"::varchar as pub_rec,
        _airbyte_data:"purpose"::varchar as purpose,
        _airbyte_data:"pymnt_plan"::varchar as pymnt_plan,
        _airbyte_data:"recoveries"::varchar as recoveries,
        _airbyte_data:"revol_bal"::varchar as revol_bal,
        _airbyte_data:"revol_util"::varchar as revol_util,
        _airbyte_data:"sub_grade"::varchar as sub_grade,
        _airbyte_data:"term"::varchar as term,
        _airbyte_data:"title"::varchar as title,
        _airbyte_data:"total_acc"::varchar as total_acc,
        _airbyte_data:"total_pymnt"::varchar as total_pymnt,
        _airbyte_data:"total_pymnt_inv"::varchar as total_pymnt_inv,
        _airbyte_data:"total_rec_int"::varchar as total_rec_int,
        _airbyte_data:"total_rec_late_fee"::varchar as total_rec_late_fee,
        _airbyte_data:"total_rec_prncp"::varchar as total_rec_prncp,
        _airbyte_data:"url"::varchar as url,
        _airbyte_data:"verification_status"::varchar as verification_status,
        _airbyte_data:"zip_code"::varchar as zip_code,
        "_AIRBYTE_AB_ID" as airbyte_ab_id,
        "_AIRBYTE_DATA" as airbyte_data,
        "_AIRBYTE_EMITTED_AT" as airbyte_emitted_at

    from raw_source

)

select * from final