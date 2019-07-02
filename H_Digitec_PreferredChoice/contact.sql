with cte_man_fields as (
	select
		field2 contact_id,
		field6 full_name,
		CASE
			WHEN CHARINDEX(' ', field6) <> 0 THEN SUBSTRING(field6, 1, CHARINDEX(' ', field6)) 
			ELSE ''
		END first_name,
		CASE
			WHEN CHARINDEX(' ', field6) <> 0 THEN SUBSTRING(field6, charindex(' ', field6) + 1, len(field6) - charindex(' ', field6))
			ELSE field6
		END last_name,
		CASE
			WHEN field40 like '%.u' then replace(field40, '.u', '.uk')
			WHEN field40 is null or replace(field40, char(13) + char(10), '') not like '%@%' or replace(field40, char(13) + char(10), '') like '@%' or replace(field40, char(13) + char(10), '') like '%@' then concat(field2, '-', replace(field6, char(32), '-'), '@noemail.com')
			ELSE replace(replace(field40, '#mailo:', ''), '#mailto;', '')
		END email
	from [Company Contact Database]
),
cte_contacts as (
	select
		con.field2 contact_id,
		com.field2 company_id,
		cte_man_fields.full_name,
		cte_man_fields.first_name,
		cte_man_fields.last_name,
		con.field7 contact_position,
		con.field8 department,
		con.field9 telephone,
		con.field38 mobile,
		cte_man_fields.email email,
		con.field40,
		con.field27 notes,
		ROW_NUMBER() OVER(PARTITION BY LOWER(cte_man_fields.email) ORDER BY CASE WHEN com.field2 IS NOT NULL THEN 1 ELSE 2 END) AS rn
	from [Company Contact Database] con
	LEFT JOIN [Company Database] com on con.field1 = com.field2
	LEFT JOIN cte_man_fields on con.field2 = cte_man_fields.contact_id
)
SELECT
COALESCE(company_id, '1') "contact-companyId",
contact_id "contact-externalId",
COALESCE(first_name, '') "contact-firstName",
CASE
	WHEN len(last_name) > 100 THEN LEFT(last_name, 99)
	WHEN last_name is null then 'Unknown'
	ELSE last_name
END "contact-lastName",
CASE
	WHEN len(contact_position) > 200 THEN left(contact_position, CHARINDEX(char(10), contact_position))
	ELSE COALESCE(contact_position, '')
END "contact-jobTitle",
COALESCE(department, '') "contact-department",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(telephone))  <> 0 THEN [dbo].[extract_number_from_str](telephone) ELSE COALESCE(RTRIM(telephone), '')
END "contact-phone",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(mobile))  <> 0 THEN [dbo].[extract_number_from_str](mobile) ELSE COALESCE(RTRIM(mobile), '')
END mobile,
CASE
	WHEN rn <> 1 THEN stuff(email, 1, 0, CASE WHEN rn = 2 THEN 'DUP_' ELSE concat('DUP', rn - 1, '_') END)
	ELSE COALESCE(email, '')
END "contact-email",

COALESCE(notes, '') "contact-Note"

from cte_contacts
-- WHERE company_id IS NULL
-- WHERE last_name = 'Bailey'