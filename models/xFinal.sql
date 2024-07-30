{{ config(
    materialized="incremental",
    unique_key='DTR')
 }}


with
    my_base as (
        select
            dtr,
            domain,
            team_region,
            primary_poc_email,
            contact_owner as contact_owner,
            create_date,
            createdat_ist,
            date(date_trunc('week', createdat_ist)) as create_week,
            date(date_trunc('month', createdat_ist)) as create_month,
            team_fta_date,
            team_ftna_date,
            team_website_chat_date,
            team_sad_date,
            mql_date,
            sao_date,
            home_country,
            home_mapping_region,
            home_geo,
            mql_flag,
            rejected_flag,
            manual_rejected_flag,
            auto_manual_rejected_flag,
            nr_mql_flag,
            workable_set_flag,
            sao_flag,
            team_lead_status,
            company_type,
            final_employee_range,
            mk_norm_attribution_channel,
            channel_break_up,
            mk___mql_mode_of_conversion,
            industry,
            company_id,
            contact_id,
            hs_created_by_user_id,
            deal_id,
            dealname,
            sao_amount,
            deal_owner,
            sl_deal_type,
            deal_stage_1 as deal_stage,
            sl_deal_source,
            hq_based,
            case
                when company_type = 'SMB'
                then 'SMB'
                when company_type = 'Mid Size' and hq_based = 'US'
                then 'MM - US'
                when company_type = 'Mid Size' and hq_based <> 'US'
                then 'MM - ROW'
            end as segment,
            sherly_rn,
            team_linkedin_date,
            team_gartner_date
        from
            (
                select
                    tb.dtr,
                    tb.dtr_2,
                    tb.domain,
                    tb.team_region,
                    tb.email as primary_poc_email,
                    tb.contact_owner,
                    tb.home_country,
                    tb.home_mapping_region,
                    tb.home_geo,
                    tb.mql_date,
                    tb.create_date,
                    tb.createdat_ist,
                    cd.team_fta_date,
                    cd.team_ftna_date,
                    cd.team_website_chat_date,
                    cd.team_sad_date,
                    tb.company_id,
                    tb.contact_id,
                    tb.hs_created_by_user_id,
                    case
                        when
                            mql_date is not null
                            and (
                                team_lead_status <> 'Invalid'
                                or team_lead_status is null
                            )
                        then 1
                    end as mql_flag,
                    team_lead_status,
                    case
                        when
                            team_lead_status = 'Disqualified'
                            and (
                                team_sl_disqualified_category
                                = 'Not the right persona,Missing contact details'
                                or team_sl_disqualified_category
                                = 'Missing contact details,Not the right persona'
                                or team_sl_disqualified_category
                                = 'Missing contact details,Not the right persona'
                                or team_sl_disqualified_category
                                = 'Not the right persona'
                            )
                        then 1
                    end as rejected_flag,
                    case
                        when team_lead_status = 'Disqualified' then 1
                    end as manual_rejected_flag,
                    case
                        when rejected_flag is not null
                        then 1
                        when manual_rejected_flag is not null
                        then 1
                    end as auto_manual_rejected_flag,
                    case
                        when mql_flag = 1 and rejected_flag is null then 1
                    end as nr_mql_flag,
                    case
                        when mql_flag = 1 and auto_manual_rejected_flag is null then 1
                    end as workable_set_flag,
                    mk_lead_source,
                    company_type,
                    mk_norm_attribution_channel,
                    channel_break_up,
                    mk___mql_mode_of_conversion,
                    dr1.deal_id as deal_id,
                    dr1.dealname as dealname,
                    ds1.sao_amount as sao_amount,
                    dr1.sao as sao_flag,
                    ds1.sao_date as sao_date,
                    dr1.deal_owner as deal_owner,
                    dr1.sl_deal_type as sl_deal_type,
                    dr1.deal_stage as deal_stage_1,
                    dr1.sl_deal_source as sl_deal_source,
                    dr1.closedate as closedate,
                    final_employee_range,
                    industry,

                    y.hq_based,
                    case
                        when tb.team_region is not null
                        then
                            row_number() over (
                                partition by tb.team_region order by tb.createdat_ist
                            )
                    end as sherly_rn,
                    team_linkedin_date,
                    team_gartner_date
                from {{ ref("stg_teams_base") }} tb
                left join {{ ref("stg_contacts_to_dtr_dates") }} cd on cd.dtr = tb.dtr
                left join {{ ref("stg_team_lead_status_cte") }} tls on tls.dtr = tb.dtr
                left join
                    {{ ref("stg_contacts_to_dtr_strings") }} cds on cds.dtr = tb.dtr
                left join {{ ref("stg_deals_sao1") }} ds1 on ds1.dtr = tb.dtr
                left join {{ ref("stg_deals_reasons1") }} dr1 on dr1.dtr = tb.dtr
                left join
                    (
                        select company_id, home_geo as hq_based
                        from
                            (
                                select
                                    hdco.id as company_id,
                                    domain,
                                    continent_mapping__na as home_mapping_region,
                                    case
                                        when
                                            home_mapping_region
                                            in ('Asia', 'India', 'Oceania', 'SEA')
                                        then 'APAC'
                                        when
                                            home_mapping_region
                                            in ('Europe', 'Middle East', 'UK', 'Africa')
                                        then 'EMEA'
                                        when
                                            home_mapping_region
                                            in ('North America', 'South America')
                                        then 'US'
                                        else '-'
                                    end as home_geo,
                                from {{ source("hubspot", "COMPANIES") }} hdco
                            )
                    ) y
                    on tb.company_id = y.company_id
            )
        where date(createdat_ist) >= date('2023-01-01')
    )

select *
from my_base

{% if is_incremental() %}
    where create_date > (select max(create_date) from {{ this }})
{% endif %}
