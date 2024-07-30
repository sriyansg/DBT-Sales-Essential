select *
from
    (
        select
            co.company_id,
            -- co.domain + coalesce(team_region, '') as dtr,
            co.domain || COALESCE(team_region, '') AS dtr,
            sl_size_of_company as final_employee_range,
            case
               WHEN m_contact_id IS NOT NULL THEN co.domain || COALESCE(team_region, '')
            end as dtr_2,
            co.domain,
            co.account_manager,
            co.industry,
            c.*
        from {{ ref("stg_COMPANIESCTE") }} co
        left join
            {{ ref("stg_CONTACT_CTE") }} c
            on round(c.associatedcompanyid) = round(co.hs_object_id)
        left join
         {{ ref("STG_MQL_CTE") }} m on round(c.contact_id) = round(m.m_contact_id)
        where contact_id is not null
    )