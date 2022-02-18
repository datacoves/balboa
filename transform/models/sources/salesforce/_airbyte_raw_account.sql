with raw_source as (

    select
        *
    from {{ source('salesforce', '_airbyte_raw_account') }}

),

final as (

    select
        _airbyte_data:"AccountNumber"::varchar as accountnumber,
        _airbyte_data:"AccountSource"::varchar as accountsource,
        _airbyte_data:"Active__c"::varchar as active__c,
        _airbyte_data:"AnnualRevenue"::varchar as annualrevenue,
        _airbyte_data:"BillingCity"::varchar as billingcity,
        _airbyte_data:"BillingCountry"::varchar as billingcountry,
        _airbyte_data:"BillingGeocodeAccuracy"::varchar as billinggeocodeaccuracy,
        _airbyte_data:"BillingLatitude"::varchar as billinglatitude,
        _airbyte_data:"BillingLongitude"::varchar as billinglongitude,
        _airbyte_data:"BillingPostalCode"::varchar as billingpostalcode,
        _airbyte_data:"BillingState"::varchar as billingstate,
        _airbyte_data:"BillingStreet"::varchar as billingstreet,
        _airbyte_data:"CleanStatus"::varchar as cleanstatus,
        _airbyte_data:"CreatedById"::varchar as createdbyid,
        _airbyte_data:"CreatedDate"::varchar as createddate,
        _airbyte_data:"CustomerPriority__c"::varchar as customerpriority__c,
        _airbyte_data:"DandbCompanyId"::varchar as dandbcompanyid,
        _airbyte_data:"Description"::varchar as description,
        _airbyte_data:"DunsNumber"::varchar as dunsnumber,
        _airbyte_data:"Fax"::varchar as fax,
        _airbyte_data:"Id"::varchar as id,
        _airbyte_data:"Industry"::varchar as industry,
        _airbyte_data:"IsDeleted"::varchar as isdeleted,
        _airbyte_data:"Jigsaw"::varchar as jigsaw,
        _airbyte_data:"JigsawCompanyId"::varchar as jigsawcompanyid,
        _airbyte_data:"LastActivityDate"::varchar as lastactivitydate,
        _airbyte_data:"LastModifiedById"::varchar as lastmodifiedbyid,
        _airbyte_data:"LastModifiedDate"::varchar as lastmodifieddate,
        _airbyte_data:"LastReferencedDate"::varchar as lastreferenceddate,
        _airbyte_data:"LastViewedDate"::varchar as lastvieweddate,
        _airbyte_data:"MasterRecordId"::varchar as masterrecordid,
        _airbyte_data:"NaicsCode"::varchar as naicscode,
        _airbyte_data:"NaicsDesc"::varchar as naicsdesc,
        _airbyte_data:"Name"::varchar as name,
        _airbyte_data:"NumberOfEmployees"::varchar as numberofemployees,
        _airbyte_data:"NumberofLocations__c"::varchar as numberoflocations__c,
        _airbyte_data:"OwnerId"::varchar as ownerid,
        _airbyte_data:"Ownership"::varchar as ownership,
        _airbyte_data:"ParentId"::varchar as parentid,
        _airbyte_data:"Phone"::varchar as phone,
        _airbyte_data:"PhotoUrl"::varchar as photourl,
        _airbyte_data:"Rating"::varchar as rating,
        _airbyte_data:"SLAExpirationDate__c"::varchar as slaexpirationdate__c,
        _airbyte_data:"SLASerialNumber__c"::varchar as slaserialnumber__c,
        _airbyte_data:"SLA__c"::varchar as sla__c,
        _airbyte_data:"ShippingCity"::varchar as shippingcity,
        _airbyte_data:"ShippingCountry"::varchar as shippingcountry,
        _airbyte_data:"ShippingGeocodeAccuracy"::varchar as shippinggeocodeaccuracy,
        _airbyte_data:"ShippingLatitude"::varchar as shippinglatitude,
        _airbyte_data:"ShippingLongitude"::varchar as shippinglongitude,
        _airbyte_data:"ShippingPostalCode"::varchar as shippingpostalcode,
        _airbyte_data:"ShippingState"::varchar as shippingstate,
        _airbyte_data:"ShippingStreet"::varchar as shippingstreet,
        _airbyte_data:"Sic"::varchar as sic,
        _airbyte_data:"SicDesc"::varchar as sicdesc,
        _airbyte_data:"Site"::varchar as site,
        _airbyte_data:"SystemModstamp"::varchar as systemmodstamp,
        _airbyte_data:"TickerSymbol"::varchar as tickersymbol,
        _airbyte_data:"Tradestyle"::varchar as tradestyle,
        _airbyte_data:"Type"::varchar as type,
        _airbyte_data:"UpsellOpportunity__c"::varchar as upsellopportunity__c,
        _airbyte_data:"Website"::varchar as website,
        _airbyte_data:"YearStarted"::varchar as yearstarted,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

