with raw_source as (

    select
        parse_json(replace(_airbyte_data::string, '"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('raw', '_airbyte_raw_country_codes') }}

),

final as (

    select
        airbyte_data_clean:"CLDR display name"::varchar as cldr_display_name,
        airbyte_data_clean:"Capital"::varchar as capital,
        airbyte_data_clean:"Continent"::varchar as continent,
        airbyte_data_clean:"DS"::varchar as ds,
        airbyte_data_clean:"Developed / Developing Countries"::varchar as developed___developing_countries,
        airbyte_data_clean:"Dial"::varchar as dial,
        airbyte_data_clean:"EDGAR"::varchar as edgar,
        airbyte_data_clean:"FIFA"::varchar as fifa,
        airbyte_data_clean:"FIPS"::varchar as fips,
        airbyte_data_clean:"GAUL"::varchar as gaul,
        airbyte_data_clean:"Geoname ID"::varchar as geoname_id,
        airbyte_data_clean:"Global Code"::varchar as global_code,
        airbyte_data_clean:"Global Name"::varchar as global_name,
        airbyte_data_clean:"IOC"::varchar as ioc,
        airbyte_data_clean:"ISO3166-1-Alpha-2"::varchar as iso3166_1_alpha_2,
        airbyte_data_clean:"ISO3166-1-Alpha-3"::varchar as iso3166_1_alpha_3,
        airbyte_data_clean:"ISO3166-1-numeric"::varchar as iso3166_1_numeric,
        airbyte_data_clean:"ISO4217-currency_alphabetic_code"::varchar as iso4217_currency_alphabetic_code,
        airbyte_data_clean:"ISO4217-currency_country_name"::varchar as iso4217_currency_country_name,
        airbyte_data_clean:"ISO4217-currency_minor_unit"::varchar as iso4217_currency_minor_unit,
        airbyte_data_clean:"ISO4217-currency_name"::varchar as iso4217_currency_name,
        airbyte_data_clean:"ISO4217-currency_numeric_code"::varchar as iso4217_currency_numeric_code,
        airbyte_data_clean:"ITU"::varchar as itu,
        airbyte_data_clean:"Intermediate Region Code"::varchar as intermediate_region_code,
        airbyte_data_clean:"Intermediate Region Name"::varchar as intermediate_region_name,
        airbyte_data_clean:"Land Locked Developing Countries (LLDC)"::varchar as land_locked_developing_countries__lldc_,
        airbyte_data_clean:"Languages"::varchar as languages,
        airbyte_data_clean:"Least Developed Countries (LDC)"::varchar as least_developed_countries__ldc_,
        airbyte_data_clean:"M49"::varchar as m49,
        airbyte_data_clean:"MARC"::varchar as marc,
        airbyte_data_clean:"Region Code"::varchar as region_code,
        airbyte_data_clean:"Region Name"::varchar as region_name,
        airbyte_data_clean:"Small Island Developing States (SIDS)"::varchar as small_island_developing_states__sids_,
        airbyte_data_clean:"Sub-region Code"::varchar as sub_region_code,
        airbyte_data_clean:"Sub-region Name"::varchar as sub_region_name,
        airbyte_data_clean:"TLD"::varchar as tld,
        airbyte_data_clean:"UNTERM Arabic Formal"::varchar as unterm_arabic_formal,
        airbyte_data_clean:"UNTERM Arabic Short"::varchar as unterm_arabic_short,
        airbyte_data_clean:"UNTERM Chinese Formal"::varchar as unterm_chinese_formal,
        airbyte_data_clean:"UNTERM Chinese Short"::varchar as unterm_chinese_short,
        airbyte_data_clean:"UNTERM English Formal"::varchar as unterm_english_formal,
        airbyte_data_clean:"UNTERM English Short"::varchar as unterm_english_short,
        airbyte_data_clean:"UNTERM French Formal"::varchar as unterm_french_formal,
        airbyte_data_clean:"UNTERM French Short"::varchar as unterm_french_short,
        airbyte_data_clean:"UNTERM Russian Formal"::varchar as unterm_russian_formal,
        airbyte_data_clean:"UNTERM Russian Short"::varchar as unterm_russian_short,
        airbyte_data_clean:"UNTERM Spanish Formal"::varchar as unterm_spanish_formal,
        airbyte_data_clean:"UNTERM Spanish Short"::varchar as unterm_spanish_short,
        airbyte_data_clean:"WMO"::varchar as wmo,
        airbyte_data_clean:"is_independent"::varchar as is_independent,
        airbyte_data_clean:"official_name_ar"::varchar as official_name_ar,
        airbyte_data_clean:"official_name_cn"::varchar as official_name_cn,
        airbyte_data_clean:"official_name_en"::varchar as official_name_en,
        airbyte_data_clean:"official_name_es"::varchar as official_name_es,
        airbyte_data_clean:"official_name_fr"::varchar as official_name_fr,
        airbyte_data_clean:"official_name_ru"::varchar as official_name_ru,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final
