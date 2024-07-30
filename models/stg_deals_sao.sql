SELECT
		dtr,
		count(DISTINCT deal_id) AS count_deals,
	min(deal_create_date_ist) AS sao_date,
	min(amount) AS sao_amount
FROM
	{{(ref('stg_deals_base'))}}
GROUP BY
	1
