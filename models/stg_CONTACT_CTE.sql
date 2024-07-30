select
    hdc.id as contact_id,
    hdc.createdat_ist,
    date(hdc.createdat_ist) as create_date,
    hdc.email,
    hso.firstname || ' ' || hso.lastname as contact_owner,
    team_region,
    hdc.mk___mql_mode_of_conversion,
    home_country,
    home_mapping_region,
    home_geo,
    company_type,
    employee_range as form_employee_range,
    associatedcompanyid,
    fta_date,
    ftna_date,
    website_chat_date,
    sad_date,
    linkedin_dt,
    gartner_dt,
    sl_disqualified_category,
    sl_disqualified_reasons,
    sl_invalid_reasons,
    sl_lead_status,
    temp_lead_status,
    least(
        fta_date, ftna_date, website_chat_date, sad_date, linkedin_dt, gartner_dt
    ) as mql_date,
    case
        when mql_date = fta_date
        then 'FTA'
        when mql_date = website_chat_date
        then 'Website Chat'
        when mql_date = sad_date
        then 'Schedule a Demo'
        when mql_date = ftna_date
        then 'FTNA'
        when mql_date = linkedin_dt
        then 'LinkedIn'
        when mql_date = gartner_dt
        then 'Gartner'
    end as mode_of_conversion,
    lead_outreach_type,
    sales___lead_outreach_category,
    hs_created_by_user_id,
    mk_lead_source,
    mk_norm_attribution_channel,
    case
        when mk_norm_attribution_channel is null
        then 'Not Available'
        else mk_norm_attribution_channel
    end as channel,
    case
        when mk_attribution_channel like '%Paid Referral%'
        then 'Paid Referral'
        when
            mk_attribution_channel like '%Paid%'
            and mk_attribution_channel not like '%Referral%'
        then 'Paid AdWords'
        else channel
    end as channel_break_up,
    mk_lead_status1
from
    (
        select
            h.id,
            h.createdate,
            sl_lead_status,
            convert_timezone('Asia/Kolkata', h.createdate) as createdat_ist,
            h.email,
            hs_lead_status,
            sdr_lead_status,
            mk___mql_mode_of_conversion,
            mapping_region as home_mapping_region,
            country,
            ip_country,
            
            coalesce(country, ip_country, '-') as home_country,
            case
                when home_mapping_region in ('Asia', 'India', 'Oceania', 'SEA')
                then 'APAC'
                when home_mapping_region in ('Europe', 'Middle East', 'UK', 'Africa')
                then 'EMEA'
                when home_mapping_region in ('North America', 'South America')
                then 'US'
                else '-'
            end as home_geo,
            mk_lead_source,
            mk_norm_employee_range,
            employee_range,
            case
                when mk_norm_employee_range like '%1000%' then 'Mid Size' else 'SMB'
            end as company_type,
            associatedcompanyid,
            date(fta_date) as fta_date,
            date(ftna_date) as ftna_date,
            date(website_chat_date) as website_chat_date,
            date(sad_date) as sad_date,
            date(linkedin_date) as linkedin_dt,
            date(gartner_date) as gartner_dt,
            sl_disqualified_category,
            sl_invalid_reasons,
            temp_mk_lead_status as mk_lead_status1,
            coalesce(mk_lead_status1, sl_lead_status) as lead_status_final,
            case
                when lower(lead_status_final) = 'disqualified lead'
                then 'disql'
                when lower(lead_status_final) like '%unqualified%'
                then 'unql'
                else lower(lead_status_final)
            end as temp_lead_status,
            sl_disqualified_reasons,
            lead_outreach_type,
            sales___lead_outreach_category,
            hs_created_by_user_id,
            mk_norm_attribution_channel,
            mk_attribution_channel,
            HUBSPOT_OWNER_ID
        from {{ source("hubspot", "CONTACTS") }} h
        where
            1 = 1
            and h.email is not null
            and lower(h.email) not like '%gmail%'
            and lower(h.email) not like '%yahoo%'
            and lower(h.email) not like '%hevotest%'
            and lower(h.email) not like '%hevodata%'
    ) hdc
left join
    (
        select team_region, email
        from {{ source("hubspot", "HEVOSECURITYUSERS_11_J7_DBT_HEVO_SECURITY_USERS") }}
    ) hsu
    on lower(hsu.email) = lower(hdc.email)
left join {{source("hubspot","HUBSPOT_OWNER_HS_OWNER")}} hso on hso.id=hdc.HUBSPOT_OWNER_ID