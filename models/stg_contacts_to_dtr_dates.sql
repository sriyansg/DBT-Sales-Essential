SELECT
		dtr,
		min(fta_date) AS team_fta_date,
		min(ftna_date) AS team_ftna_date,
	min(website_chat_date) AS team_website_chat_date,
	min(sad_date) AS team_sad_date,
min(linkedin_dt) AS team_linkedin_date,
min(gartner_dt) AS team_gartner_date
FROM
	{{ref('stg_base')}}
GROUP BY
	1
    