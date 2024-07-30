SELECT
    hdco.id AS company_id,
    domain,
    hdco.createdate,
    hs_object_id,
    industry,
    sl_size_of_company,
    hdo2.firstname || ' ' || hdo2.lastname AS account_manager  -- Removed the extra comma here
FROM {{ source("hubspot", "COMPANIES") }} hdco
LEFT JOIN {{ source("hubspot", "HUBSPOT_OWNER_HS_OWNER") }} hdo2 ON hdo2.id = hdco.account_manager___company
WHERE
    LOWER(domain) NOT LIKE '%hevo%'
