SELECT
cl.vc_company_id,
NULLIF(cl.longitude, 0) longitude,
NULLIF(cl.latitude, 0) latitude,
mci.external_id
FROM company_locations cl
LEFT JOIN mapping_company_id mci ON cl.vc_company_id = mci.company_id
JOIN [Company Database] cd ON mci.external_id = cd.field2
-- WHERE NULLIF(cl.longitude, 0)
-- AND NULLIF(cl.latitude, 0) <> 0