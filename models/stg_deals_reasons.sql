SELECT
		*
	FROM (
		SELECT
			dtr,
			deal_id,
			dealname,
			sao,
			amount,
			deal_owner,
			closed_lost_category,
			closed_lost_reason,
			
			deal_stage,
			sl_deal_source,
		
			closedate,
			sl_deal_type,
			solutions_assigned_hdd,
			row_number() OVER (PARTITION BY dtr ORDER BY deal_create_date_ist DESC) AS rn
		FROM
			{{ref("stg_deals_base")}})
	WHERE
		rn = 1
