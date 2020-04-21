--CHECK GDPR
select *
from candidate_gdpr_compliance --max id: 30

--MAIN SCRIPT
SELECT
	id as candidate_id
	, gdpr_ok_c
	, gdpr_ok_date_c::TIMESTAMP
	, 1 explicit_consent
	, 3 exercise_right
	, 6 request_through
	, gdpr_ok_date_c::TIMESTAMP AS request_through_date
	, 6 obtained_through
	, gdpr_ok_date_c::TIMESTAMP AS obtained_through_date
	, 1 expire
	, '2024-05-29 00:00:00'::TIMESTAMP expire_date
	, -10 obtained_by
	, CURRENT_TIMESTAMP AS insert_timestamp
FROM contact
WHERE gdpr_ok_c = '1'
AND recordtypeid IN ('0120Y0000013O5c','0120Y000000RZZV')