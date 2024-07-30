select
    hdc.id as m_contact_id,
    hdc.createdat_ist as m_createdat_ist,
    date(hdc.createdat_ist) as m_create_date,
    hdc.email as m_email,
    hsu.team_region as m_team_region,
    associatedcompanyid as m_associatedcompanyid,
    fta_date as m_fta_date,
    ftna_date as m_ftna_date,
    website_chat_date as m_website_chat_date,
    sad_date as m_sad_date,
    linkedin_dt as m_linkedin_dt,
    gartner_dt as m_gartner_dt,
    case
        when
            least(
                coalesce(m_fta_date, '9999-12-31'),
                coalesce(m_ftna_date, '9999-12-31'),
                coalesce(m_website_chat_date, '9999-12-31'),
                coalesce(m_sad_date, '9999-12-31'),
                coalesce(m_linkedin_dt, '9999-12-31'),
                coalesce(m_gartner_dt, '9999-12-31')
            )
            = '9999-12-31'
        then null
        else
            least(
                coalesce(m_fta_date, '9999-12-31'),
                coalesce(m_ftna_date, '9999-12-31'),
                coalesce(m_website_chat_date, '9999-12-31'),
                coalesce(m_sad_date, '9999-12-31'),
                coalesce(m_linkedin_dt, '9999-12-31'),
                coalesce(m_gartner_dt, '9999-12-31')
            )
    end as m_mql_date
from
    (
        select
            h.id,
            h.createdate,
            sl_lead_status,
            convert_timezone('Asia/Kolkata', h.createdate) as createdat_ist,
            h.email,
            associatedcompanyid,
            date(fta_date) as fta_date,
            date(ftna_date) as ftna_date,
            date(website_chat_date) as website_chat_date,
            date(sad_date) as sad_date,
            date(linkedin_date) as linkedin_dt,
            date(gartner_date) as gartner_dt,
            temp_mk_lead_status,
            coalesce(temp_mk_lead_status, sl_lead_status) as lead_status_final,
            case
                when lower(lead_status_final) = 'disqualified lead'
                then 'disql'
                when lower(lead_status_final) like '%unqualified%'
                then 'unql'
                else lower(lead_status_final)
            end as temp_lead_status
        from {{ source("hubspot", "CONTACTS") }} h
        where
            1 = 1
            -- and date(h.createdat) >= date('2023-01-01')
            and h.email is not null
            and lower(h.email) not like '%gmail%'
            and lower(h.email) not like '%yahoo%'
            and lower(h.email) not like '%hevotest%'
            and lower(h.email) not like '%hevodata%'
    ) hdc
left join
    (
        select team_region, email
        from
            {{
                source(
                    "hubspot",
                    "HEVOSECURITYUSERS_11_J7_DBT_HEVO_SECURITY_USERS",
                )
            }}

    ) hsu
    on lower(hsu.email) = lower(hdc.email)
