with cte_man_fields as (
	select
		field2 candidate_id,
		field1 full_name,
		CASE
			WHEN CHARINDEX(' ', field1) <> 0 THEN SUBSTRING(field1, 1, CHARINDEX(' ', field1)) 
			ELSE field1
		END first_name,
		CASE
			WHEN CHARINDEX(' ', field1) <> 0 and len(field1) - charindex(' ', field1) > 0 THEN SUBSTRING(field1, charindex(' ', field1) + 1, len(field1) - charindex(' ', field1))
			ELSE field1
		END last_name,
		CASE
			WHEN field32 like '%.u' then replace(field32, '.u', '.uk')
			WHEN field32 is null or field32 not like '%@%' then concat(field2, '-', replace(field1,char(32),'-'), '@noemail.com')
			ELSE field32
		END primary_email
		FROM [Candidates Database]
		WHERE field1 NOT LIKE '%?%'
),
cte_country as (
	select 
	field2 candidate_id,
	case 
		when RTRIM(field31) in ('London', 'Buckinghamshire', 'West Sussex') then 'England'
		when RTRIM(field31) in ('Amsterdam') then 'Holland'
		when RTRIM(replace(replace(field31, '  ',' '), char(13) + char(10), '')) in ('United Kindom') then 'United Kingdom'
		when RTRIM(field31) = 'Uae' then 'United Arab Emirates'
		else RTRIM(replace(replace(field31, '  ',' '), char(13) + char(10), ''))
	end country,
	CASE
		WHEN RTRIM(field22) = 'German' THEN 'DE'
		WHEN RTRIM(field22) = 'Indian' THEN 'IN'
		WHEN RTRIM(field22) = 'Irish' THEN 'IE'
		WHEN RTRIM(field22) = 'South African' THEN 'ZA'
		WHEN RTRIM(field22) = 'New Zealand' THEN 'NZ'
		WHEN RTRIM(field22) = 'Iranian' THEN 'IR'
		WHEN RTRIM(field22) = 'French' THEN 'FR'
		WHEN RTRIM(field22) = 'Australian' THEN 'AU'
		WHEN RTRIM(field22) = 'American' THEN 'US'
		WHEN RTRIM(field22) = 'Turkish' THEN 'TR'
		WHEN RTRIM(field22) = 'Slovakian' THEN 'SK'
		WHEN RTRIM(field22) = 'Nigerian' THEN 'NG'
		WHEN RTRIM(field22) = 'Ukrainian' THEN 'UA'
		WHEN RTRIM(field22) = 'Dutch' THEN 'NL'
		WHEN RTRIM(field22) = 'Italian' THEN 'IT'
		WHEN RTRIM(field22) = 'Greek' THEN 'GR'
		WHEN RTRIM(field22) = 'Bulgarian' THEN 'BG'
		WHEN RTRIM(field22) = 'Swedish' THEN 'SE'
		WHEN RTRIM(field22) = 'Brazilian' THEN 'BR'
		WHEN RTRIM(field22) = 'Canadian' THEN 'CA'
		WHEN RTRIM(field22) = 'Austrian' THEN 'AT'
		WHEN RTRIM(field22) = 'Zimbabwe' THEN 'ZW'
		WHEN RTRIM(field22) = 'Pakistani' THEN 'PK'
		WHEN RTRIM(field22) = 'Spanish' THEN 'ES'
		WHEN RTRIM(field22) = 'Ugandan' THEN 'UG'
		WHEN RTRIM(field22) = 'Mexican' THEN 'MX'
		WHEN RTRIM(field22) = 'British' THEN 'GB'
		WHEN RTRIM(field22) = 'Malaysian' THEN 'MY'
		WHEN RTRIM(field22) = 'Czech' THEN 'CZ'
		WHEN RTRIM(field22) = 'Hungarian' THEN 'HU'
		WHEN RTRIM(field22) = 'Chinese' THEN 'CN'
		WHEN RTRIM(field22) = 'Norweigen' THEN 'NO'
		WHEN RTRIM(field22) = 'Russian' THEN 'RU'
		WHEN RTRIM(field22) = 'Jamaican' THEN 'JM'
		WHEN RTRIM(field22) = 'Egyptian' THEN 'EG'
		WHEN RTRIM(field22) = 'Polish' THEN 'PL'
		WHEN RTRIM(field22) = 'Argentinean' THEN 'AR'
		WHEN RTRIM(field22) = 'Finish' THEN 'FI'
		WHEN RTRIM(field22) = 'Danish' THEN 'DK'
		WHEN RTRIM(field22) = 'Belgium' THEN 'BE'
		WHEN RTRIM(field22) = 'Israeli' THEN 'IL'
		WHEN RTRIM(field22) = 'Portuguese' THEN 'PT'
		WHEN RTRIM(field22) = 'Romanian' THEN 'RO'
		WHEN RTRIM(field22) = 'Yugoslavian' THEN ''
	END citizenship
	from [Candidates Database]
),
cte_candidates as (
	select
	field2 candidate_id,
	field5 registration_date,
	field29 last_contact,
	field40 record_updated,
	field41 cv_updated,
	cte_man_fields.full_name,
	cte_man_fields.first_name,
	cte_man_fields.last_name,
	ROW_NUMBER() OVER(PARTITION BY field2, cte_man_fields.first_name, cte_man_fields.last_name ORDER BY field2) rn_test,
	field3 dob,
	citizenship,
	replace(field6, char(13)+char(10), '') street_1,
	replace(field7, char(13)+char(10), '') street_2,
	replace(field30, char(13)+char(10), '') town,
	replace(field8, char(13)+char(10), '') county,
	replace(cte_country.country, char(13)+char(10), '') country,
	cc.country_code,
	field9 postcode,
	case
		when charindex(char(13) + char(10), field10) <> 0 then case
																															when replace(field10, char(13) + char(10), '#') not like '#%'
																																then RTRIM(LEFT(replace(field10, char(13) + char(10), '#'), charindex('#', replace(field10, char(13) + char(10), '#')) - 1))
																															else 
																																case 
																																	when len(replace(field10, char(13) + char(10), '')) > 12 then RTRIM(left(replace(field10, char(13) + char(10), ''), charindex(char(32), replace(field10, char(13) + char(10), ''))))
																																	else replace(field10, char(13) + char(10), '')
																																end
																														end
																															
		else RTRIM(replace(field10, '.', char(32)))
	end home_phone,
		case
		when charindex(char(13) + char(10), field11) <> 0 then case
																															when replace(field11, char(13) + char(10), '#') not like '#%'
																																then RTRIM(LEFT(replace(field11, char(13) + char(10), '#'), charindex('#', replace(field11, char(13) + char(10), '#')) - 1))
																															else 
																																case 
																																	when len(replace(field11, char(13) + char(10), '')) > 12 then RTRIM(left(replace(field11, char(13) + char(10), ''), charindex(char(32), replace(field11, char(13) + char(10), ''))))
																																	else replace(field11, char(13) + char(10), '')
																																end
																														end
																															
		else RTRIM(replace(replace(field11, '.', char(32)), '=', ''))
	end work_phone,
	case
		when charindex(char(13) + char(10), field34) <> 0 then case
																															when replace(field34, char(13) + char(10), '#') not like '#%'
																																then RTRIM(LEFT(replace(field34, char(13) + char(10), '#'), charindex('#', replace(field34, char(13) + char(10), '#')) - 1))
																															else 
																																case 
																																	when len(replace(field34, char(13) + char(10), '')) > 12 then RTRIM(left(replace(field34, char(13) + char(10), ''), charindex(char(32), replace(field34, char(13) + char(10), ''))))
																																	else replace(field34, char(13) + char(10), '')
																																end
																														end
																															
		else RTRIM(replace(field34, '.', char(32)))
	end primary_phone,
	cte_man_fields.primary_email primary_email,
	field28 website,
	case
		when right(RTRIM(CONVERT(NVARCHAR(MAX), field13)), 1) = ','	then replace(CONVERT(NVARCHAR(MAX), field13), ',', '')
		when charindex(',', field13) = 1 then replace(CONVERT(NVARCHAR(MAX), field13), ', ', '')
		else RTRIM(CONVERT(NVARCHAR(MAX), field13))
	end current_job_position,
-- 	case
-- 		when charindex(',', field13) <> 0 then case
-- 																						when charindex(',', field13) = 1 then replace(CONVERT(NVARCHAR(MAX), field13), ', ', '')
-- 																						else substring(field13, 1, charindex(',', field13)-1)
-- 																					 end
-- 		else field13
-- 	end current_job_position1,
-- 	case
-- 		when charindex(',', field13) <> 0 and len(CONVERT(NVARCHAR(MAX), field13)) - charindex(',', field13) > 0 then substring(field13, charindex(',', field13) + 1, len(CONVERT(NVARCHAR(MAX), field13)) - charindex(',', field13))
-- 		else field13
-- 	end current_job_position2,
	field14 industry,
	field15 skills,
	field19 contract_rate,
	field21 current_salary,
	field25 employee_status,
	field24 source,
	field56 GDPR,
	Field47 ni_number,
	ROW_NUMBER() OVER(PARTITION BY cte_man_fields.primary_email ORDER BY can.field2) AS rn_email,
	ROW_NUMBER() OVER(PARTITION BY cte_man_fields.first_name, cte_man_fields.last_name, cte_man_fields.primary_email ORDER BY can.field2) AS rn
	from [Candidates Database] can
	left join cte_man_fields on can.field2 = cte_man_fields.candidate_id
	left join cte_country on can.field2 = cte_country.candidate_id
	left join country_code cc on cc.country_name = (case
																										when cte_country.country in ('England', 'Wales') then 'United Kingdom'
																										when cte_country.country in ('Holland') then 'Netherlands'
																										else cte_country.country
																									end)
)

select
candidate_id "candidate-externalId",
first_name"candidate-firstName",
CASE 
	WHEN rn <> 1 THEN concat(last_name, ' ', rn) 
	ELSE last_name
END "candidate-Lastname",
COALESCE(format(dob, 'yyyy-MM-dd'), '') "candidate-dob",
citizenship "candidate-citizenship",
current_job_position "candidate-jobTitle1",
concat(
	CASE 
		WHEN street_1 is not null and street_1 <> '' THEN
			CASE
				WHEN RIGHT(RTRIM(street_1), 1) IN (',', '.') THEN concat(SUBSTRING(RTRIM(street_1),1,LEN(RTRIM(street_1)) - 1), ', ') 
				ELSE concat(street_1,', ')
			END
	END,
	CASE 
		WHEN street_2 is not null and street_2 <> '' THEN
			CASE 
				WHEN RIGHT(RTRIM(street_2), 1) IN (',', '.') THEN concat(SUBSTRING(RTRIM(street_2),1,LEN(RTRIM(street_2)) - 1), ', ') 
				ELSE concat(street_2,', ')
			END
	END,
	CASE WHEN town is not null THEN concat(town, ', ') END,
	CASE WHEN county is not null THEN concat(county, ', ') END,
	CASE WHEN country is not null THEN country END
) "candidate-address",
COALESCE(town, '') "candidate-city",
COALESCE(county, '') "candidate-State",
COALESCE(country_code, '') "candidate-Country",
COALESCE(postcode, '') "candidate-zipCode",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(home_phone))  <> 0 THEN replace([dbo].[extract_number_from_str](home_phone), '()', '') ELSE COALESCE(RTRIM(home_phone), '')
END "candidate-homePhone",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(work_phone))  <> 0 THEN replace(replace([dbo].[extract_number_from_str](work_phone), '()', ''), '(#)', '') ELSE COALESCE(RTRIM(work_phone), '')
END "candidate-workPhone",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(primary_phone))  <> 0 THEN replace([dbo].[extract_number_from_str](primary_phone), '()','') ELSE COALESCE(RTRIM(primary_phone), '')
END "candidate-phone",
CASE
	WHEN rn_email <> 1 THEN stuff(primary_email, 1, 0, CASE WHEN rn_email = 2 THEN 'DUP_' ELSE concat('DUP', rn_email - 1, '_') END)
	ELSE COALESCE(primary_email, '')
END "candidate-email",
-- rn_email,
COALESCE(skills, '') "candidate-skills",
contract_rate "candidate-contractRate",
current_salary "candidate-currentSalary",
'GBP' "candidate-currency",
-- concat(
-- 	candidate_id,
-- 	'.doc'
-- ) "candidate-resume",
concat(
	case when last_contact is not null then concat('Replication ID: ', candidate_id, char(10)) end,
	case when last_contact is not null then concat('Last contact: ', format(last_contact, 'yyyy-MM-dd'), char(10)) end,
	case when record_updated is not null then concat('Record updated: ', format(record_updated, 'yyyy-MM-dd'), char(10)) end,
	case when employee_status is not null then concat('Notice period: ', employee_status, char(10)) end,
	case when ni_number is not null then concat('NI number: ', ni_number) end
) "candidate-note",
GDPR

from cte_candidates
WHERE last_name IS NOT NULL