
  create or replace   view BALBOA_STAGING.L1_LOANS.personal_loans
  
    
    
(
  
    "TOTAL_ACC" COMMENT $$The total number of credit lines currently in the borrower's credit file$$, 
  
    "ANNUAL_INC" COMMENT $$The borrower's annual income$$, 
  
    "EMP_LENGTH" COMMENT $$Employment length in years$$, 
  
    "DESC" COMMENT $$Loan description provided by the borrower$$, 
  
    "TOTAL_PYMNT" COMMENT $$Payments received to date for total amount funded$$, 
  
    "LAST_PYMNT_D" COMMENT $$Last month payment was received$$, 
  
    "ADDR_STATE" COMMENT $$The state in which the borrower resides$$, 
  
    "NEXT_PYMNT_D" COMMENT $$Next scheduled payment date$$, 
  
    "EMP_TITLE" COMMENT $$The job title supplied by the borrower when applying for the loan$$, 
  
    "COLLECTION_RECOVERY_FEE" COMMENT $$Post charge off collection fee$$, 
  
    "MTHS_SINCE_LAST_MAJOR_DEROG" COMMENT $$Months since most recent 90-day or worse rating$$, 
  
    "INQ_LAST_6MTHS" COMMENT $$The number of inquiries in past 6 months (excluding auto and mortgage inquiries)$$, 
  
    "SUB_GRADE" COMMENT $$LC assigned loan subgrade$$, 
  
    "FUNDED_AMNT_INV" COMMENT $$The total amount committed by investors for that loan at that point in time$$, 
  
    "DELINQ_2YRS" COMMENT $$The number of 30+ days past-due incidences of delinquency in the borrower's credit file for the past 2 years$$, 
  
    "LOAN_ID" COMMENT $$A unique identifier for the loan$$, 
  
    "FUNDED_AMNT" COMMENT $$The total amount committed to that loan at that point in time$$, 
  
    "VERIFICATION_STATUS" COMMENT $$Indicates if income was verified by LC, not verified, or if the income source was verified$$, 
  
    "DTI" COMMENT $$The borrower's debt-to-income ratio$$, 
  
    "TOTAL_REC_PRNCP" COMMENT $$Principal received to date$$, 
  
    "GRADE" COMMENT $$LC assigned loan grade$$, 
  
    "HOME_OWNERSHIP" COMMENT $$The home ownership status provided by the borrower during registration$$, 
  
    "ISSUE_D" COMMENT $$The month which the loan was funded$$, 
  
    "MTHS_SINCE_LAST_DELINQ" COMMENT $$The number of months since the borrower's last delinquency$$, 
  
    "OUT_PRNCP" COMMENT $$Remaining outstanding principal for total amount funded$$, 
  
    "PUB_REC" COMMENT $$Number of derogatory public records$$, 
  
    "INT_RATE" COMMENT $$Interest rate on the loan$$, 
  
    "ZIP_CODE" COMMENT $$The first 3 numbers of the zip code provided by the borrower in the loan application$$, 
  
    "OPEN_ACC" COMMENT $$The number of open credit lines in the borrower's credit file$$, 
  
    "TERM" COMMENT $$The number of payments on the loan. Values are in months and can be either 36 or 60$$, 
  
    "PYMNT_PLAN" COMMENT $$Indicates if a payment plan has been put in place for the loan$$, 
  
    "URL" COMMENT $$URL for the LC page with listing data$$, 
  
    "REVOL_BAL" COMMENT $$Total credit revolving balance$$, 
  
    "RECOVERIES" COMMENT $$Post charge off gross recovery$$, 
  
    "LAST_PYMNT_AMNT" COMMENT $$Last total payment amount received$$, 
  
    "LOAN_AMNT" COMMENT $$The listed amount of the loan applied for by the borrower$$, 
  
    "PURPOSE" COMMENT $$A category provided by the borrower for the loan request$$, 
  
    "INITIAL_LIST_STATUS" COMMENT $$The initial listing status of the loan$$, 
  
    "TOTAL_REC_INT" COMMENT $$Interest received to date$$, 
  
    "TOTAL_PYMNT_INV" COMMENT $$Payments received to date for portion of total amount funded by investors$$, 
  
    "MTHS_SINCE_LAST_RECORD" COMMENT $$The number of months since the last public record$$, 
  
    "LAST_CREDIT_PULL_D" COMMENT $$The most recent month LC pulled credit for this loan$$, 
  
    "TOTAL_REC_LATE_FEE" COMMENT $$Late fees received to date$$, 
  
    "MEMBER_ID" COMMENT $$A unique identifier for the borrower$$, 
  
    "POLICY_CODE" COMMENT $$Publicly available$$, 
  
    "TITLE" COMMENT $$The loan title provided by the borrower$$, 
  
    "LOAN_STATUS" COMMENT $$Current status of the loan$$, 
  
    "INSTALLMENT" COMMENT $$The monthly payment owed by the borrower if the loan originates$$, 
  
    "EARLIEST_CR_LINE" COMMENT $$The month the borrower's earliest reported credit line was opened$$, 
  
    "REVOL_UTIL" COMMENT $$Revolving line utilization rate, or the amount of credit the borrower is using relative to all available revolving credit$$, 
  
    "OUT_PRNCP_INV" COMMENT $$Remaining outstanding principal for portion of total amount funded by investors$$, 
  
    "COLLECTIONS_12_MTHS_EX_MED" COMMENT $$Number of collections in the last 12 months excluding medical collections$$
  
)

  copy grants as (
    

with raw_source as (

    select *
    from RAW.LOANS.PERSONAL_LOANS

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
  );

