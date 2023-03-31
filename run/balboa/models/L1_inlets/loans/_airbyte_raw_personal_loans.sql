
  create or replace  view BALBOA_STAGING.l1_loans._airbyte_raw_personal_loans
  
    
    
(
  
    "ADDR_STATE" COMMENT $$The state in which the borrower resides$$, 
  
    "ANNUAL_INC" COMMENT $$The borrower's annual income$$, 
  
    "COLLECTIONS_12_MTHS_EX_MED" COMMENT $$Number of collections in the last 12 months excluding medical collections$$, 
  
    "COLLECTION_RECOVERY_FEE" COMMENT $$Post charge off collection fee$$, 
  
    "DELINQ_2YRS" COMMENT $$The number of 30+ days past-due incidences of delinquency in the borrower's credit file for the past 2 years$$, 
  
    "DESC" COMMENT $$Loan description provided by the borrower$$, 
  
    "DTI" COMMENT $$The borrower's debt-to-income ratio$$, 
  
    "EARLIEST_CR_LINE" COMMENT $$The month the borrower's earliest reported credit line was opened$$, 
  
    "EMP_LENGTH" COMMENT $$Employment length in years$$, 
  
    "EMP_TITLE" COMMENT $$The job title supplied by the borrower when applying for the loan$$, 
  
    "FUNDED_AMNT" COMMENT $$The total amount committed to that loan at that point in time$$, 
  
    "FUNDED_AMNT_INV" COMMENT $$The total amount committed by investors for that loan at that point in time$$, 
  
    "GRADE" COMMENT $$LC assigned loan grade$$, 
  
    "HOME_OWNERSHIP" COMMENT $$The home ownership status provided by the borrower during registration$$, 
  
    "INITIAL_LIST_STATUS" COMMENT $$The initial listing status of the loan$$, 
  
    "INQ_LAST_6MTHS" COMMENT $$The number of inquiries in past 6 months (excluding auto and mortgage inquiries)$$, 
  
    "INSTALLMENT" COMMENT $$The monthly payment owed by the borrower if the loan originates$$, 
  
    "INT_RATE" COMMENT $$Interest rate on the loan$$, 
  
    "ISSUE_D" COMMENT $$The month which the loan was funded$$, 
  
    "LAST_CREDIT_PULL_D" COMMENT $$The most recent month LC pulled credit for this loan$$, 
  
    "LAST_PYMNT_AMNT" COMMENT $$Last total payment amount received$$, 
  
    "LAST_PYMNT_D" COMMENT $$Last month payment was received$$, 
  
    "LOAN_AMNT" COMMENT $$The listed amount of the loan applied for by the borrower$$, 
  
    "LOAN_ID" COMMENT $$A unique identifier for the loan$$, 
  
    "LOAN_STATUS" COMMENT $$Current status of the loan$$, 
  
    "MEMBER_ID" COMMENT $$A unique identifier for the borrower$$, 
  
    "MTHS_SINCE_LAST_DELINQ" COMMENT $$The number of months since the borrower's last delinquency$$, 
  
    "MTHS_SINCE_LAST_MAJOR_DEROG" COMMENT $$Months since most recent 90-day or worse rating$$, 
  
    "MTHS_SINCE_LAST_RECORD" COMMENT $$The number of months since the last public record$$, 
  
    "NEXT_PYMNT_D" COMMENT $$Next scheduled payment date$$, 
  
    "OPEN_ACC" COMMENT $$The number of open credit lines in the borrower's credit file$$, 
  
    "OUT_PRNCP" COMMENT $$Remaining outstanding principal for total amount funded$$, 
  
    "OUT_PRNCP_INV" COMMENT $$Remaining outstanding principal for portion of total amount funded by investors$$, 
  
    "POLICY_CODE" COMMENT $$Publicly available$$, 
  
    "PUB_REC" COMMENT $$Number of derogatory public records$$, 
  
    "PURPOSE" COMMENT $$A category provided by the borrower for the loan request$$, 
  
    "PYMNT_PLAN" COMMENT $$Indicates if a payment plan has been put in place for the loan$$, 
  
    "RECOVERIES" COMMENT $$Post charge off gross recovery$$, 
  
    "REVOL_BAL" COMMENT $$Total credit revolving balance$$, 
  
    "REVOL_UTIL" COMMENT $$Revolving line utilization rate, or the amount of credit the borrower is using relative to all available revolving credit$$, 
  
    "SUB_GRADE" COMMENT $$LC assigned loan subgrade$$, 
  
    "TERM" COMMENT $$The number of payments on the loan. Values are in months and can be either 36 or 60$$, 
  
    "TITLE" COMMENT $$The loan title provided by the borrower$$, 
  
    "TOTAL_ACC" COMMENT $$The total number of credit lines currently in the borrower's credit file$$, 
  
    "TOTAL_PYMNT" COMMENT $$Payments received to date for total amount funded$$, 
  
    "TOTAL_PYMNT_INV" COMMENT $$Payments received to date for portion of total amount funded by investors$$, 
  
    "TOTAL_REC_INT" COMMENT $$Interest received to date$$, 
  
    "TOTAL_REC_LATE_FEE" COMMENT $$Late fees received to date$$, 
  
    "TOTAL_REC_PRNCP" COMMENT $$Principal received to date$$, 
  
    "URL" COMMENT $$URL for the LC page with listing data$$, 
  
    "VERIFICATION_STATUS" COMMENT $$Indicates if income was verified by LC, not verified, or if the income source was verified$$, 
  
    "ZIP_CODE" COMMENT $$The first 3 numbers of the zip code provided by the borrower in the loan application$$, 
  
    "AIRBYTE_AB_ID" COMMENT $$The Airbyte-specific ID for the record$$, 
  
    "AIRBYTE_DATA" COMMENT $$Airbyte-specific metadata for the record$$, 
  
    "AIRBYTE_EMITTED_AT" COMMENT $$The timestamp when the record was emitted by Airbyte$$
  
)

  copy grants as (
    with raw_source as (

    select *
    from RAW.LOANS._AIRBYTE_RAW_PERSONAL_LOANS
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
  );
