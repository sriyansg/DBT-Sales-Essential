SELECT
		dtr,
		listagg (DISTINCT sl_disqualified_category, ',') AS team_sl_disqualified_category,
		listagg (DISTINCT sl_disqualified_reasons, ',') AS team_sl_disqualified_reason,
		listagg (DISTINCT sl_invalid_reasons, ',') AS team_sl_invalid_reason
	FROM
		{{ref('stg_base')}}
	GROUP BY
		1
