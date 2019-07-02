WITH sub_status AS (
SELECT 'Long List' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Initial Contact' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Qualifying' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Withdrew' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Internal Interview' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Client Interview' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Client Interview Withdrew' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
UNION
SELECT 'Accepted' AS sub_status, CURRENT_TIMESTAMP AS insert_timestamp
)

SELECT
ROW_NUMBER() OVER() + 45 id,
*,
CASE
	WHEN sub_status = 'Client Interview Withdrew' THEN '#f9b3a7'
	WHEN sub_status = 'Client Interview' THEN '#bde2f9'
	WHEN sub_status = 'Initial Contact' THEN '#595959'
	WHEN sub_status = 'Withdrew' THEN '#f9b3a7'
	WHEN sub_status = 'Long List' THEN '#595959'
	WHEN sub_status = 'Accepted' THEN '#80f792'
	WHEN sub_status = 'Qualifying' THEN '#595959'
	WHEN sub_status = 'Internal Interview' THEN '#bde2f9'
END color_code,
-10 AS user
FROM sub_status