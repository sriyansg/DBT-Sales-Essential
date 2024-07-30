SELECT
		*
	FROM (
		SELECT
			*,
			row_number() OVER (PARTITION BY dtr_2 ORDER BY createdat_ist ASC) AS rn
		FROM
			{{ref('stg_base')}})
	WHERE
		rn = 1
