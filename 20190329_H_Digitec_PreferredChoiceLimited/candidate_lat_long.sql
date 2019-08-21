SELECT
cl.vc_candidate_id,
NULLIF(cl.longitude, 0) longitude,
NULLIF(cl.latitude, 0) latitude,
mci.external_id
FROM candidate_locations cl
LEFT JOIN mapping_candidate_id mci ON cl.vc_candidate_id = mci.candidate_id
JOIN [Candidates Database] cd ON mci.external_id = cd.field2