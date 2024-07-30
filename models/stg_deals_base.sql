select
    c.dtr,
    c.contact_id,
    c.team_region,
    hdd.deal_id,
    dealname,
    amount,
    sao,
    hdd.deal_create_date_ist,
    deal_owner,
    sl_deal_type,
    closed_lost_category,
    closed_lost_reason,
    -- sl_closed_lost_admin,
    hdd_s.label as deal_stage,
    sl_deal_source,
    solutions_assigned_hdd,
    -- solution_validated,
    -- solution_validation_date,
    closedate
from {{ ref("stg_base") }} c
-- left join hubspot_direct_deal_contact_association h on h.contactid = c.contact_id
left join
    (
        select distinct
            hdd.id as deal_id,
            dealname,
            sao,
            amount,
            hdo.firstname || ' ' || hdo.lastname as deal_owner,
            convert_timezone('Asia/Kolkata', hdd.createdate) as deal_create_date_ist,
            sl_deal_type,
            closed_lost_category,
            closed_lost_reason,
            -- sl_closed_lost_admin,
            dealstage,
            sl_deal_source,
            -- solution_validated,
            -- solution_validation_date,
            hdo2.firstname || ' ' || hdo2.lastname as solutions_assigned_hdd,
            sl___team_region,
            closedate
        from {{ source("hubspot", "DEALS") }} hdd
        left join
            {{ source("hubspot", "HUBSPOT_OWNER_HS_OWNER") }} hdo
            on hdd.hubspot_owner_id = hdo.id
        left join
            {{ source("hubspot", "HUBSPOT_OWNER_HS_OWNER") }} hdo2
            on hdd.sl_se_assigned = hdo2.id
        where pipeline = 32094871 and hdd.archived = 'false' and sao is not null
    ) hdd
    on hdd.sl___team_region = c.team_region
-- on hdd.deal_id = h.dealid
left join
    (
        select *
        from
            (
                select distinct
                    id,
                    label,
                    row_number() over (partition by id order by updated_at desc) as rn
                from {{ source("hubspot", "DEALS_PIPELINE_STAGES") }}
            )
        where rn = 1
    ) hdd_s
    on (hdd_s.id = hdd.dealstage)
