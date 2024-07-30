
	SELECT
		dtr,
		listagg (DISTINCT lower(temp_lead_status), ',') AS lead_status_agg,
	CASE WHEN lead_status_agg LIKE '%qualified%' THEN
		'SQL'
	WHEN lead_status_agg LIKE '%discussion%' THEN
		'In-discussion'
	WHEN lead_status_agg LIKE '%pursuit%' THEN
		'In-pursuit'
	WHEN lead_status_agg LIKE '%initiated%' THEN
		'Not Initiated'
	WHEN lead_status_agg LIKE '%nurture%' THEN
		'Nurture'
	WHEN lead_status_agg LIKE '%unql%' THEN
		'Unqualified'
	WHEN lead_status_agg LIKE '%disql%' THEN
		'Disqualified'
	WHEN lead_status_agg LIKE '%invalid%' THEN
		'Invalid'
	END AS team_lead_status
FROM
	{{ref('stg_base')}}
GROUP BY
	1
