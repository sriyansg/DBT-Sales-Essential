select
    c.dtr,
    c.contact_id,
    c.team_region,
    hdd.deal_id,
    dealname,
    amount,
    sao,
    deal_create_date_ist,
    deal_owner,
    sl_deal_type,
    closed_lost_category,
    closed_lost_reason,
    hdd_s.label as deal_stage,
    sl_deal_source,
    -- solutions_assigned_hdd,
    -- solution_validated,
    -- solution_validation_date,
    closedate
from {{ ref("stg_base") }} c
left join
    {{ source("hubspot", "DEAL_CONTACT_ASSOCIATION") }} h on h.contact_id = c.contact_id
left join
    (
        select distinct
            hdd.id as deal_id,
            dealname,
            sao,
            amount,
            convert_timezone('Asia/Kolkata', hdd.createdate) as deal_create_date_ist,
            hdo.firstname || ' ' || hdo.lastname as deal_owner,
            sl_deal_type,
            closed_lost_category,
            closed_lost_reason,
            dealstage,
            sl_deal_source,
            sl___team_region,
            hdo2.firstname || ' ' || hdo2.lastname as solutions_assigned_hdd,
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
    on hdd.deal_id = h.deal_id
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
where hdd.deal_id is not null
