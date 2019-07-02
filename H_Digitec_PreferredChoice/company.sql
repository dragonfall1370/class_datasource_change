DROP TABLE IF EXISTS _01_company_sample;
with company as(
select
field2 company_id,
field37 sequence_number,
field1 company_name,
ROW_NUMBER() OVER(PARTITION BY field1 ORDER BY field2) AS rn,
field46 record_owner,
tbl.con_email record_owner_email,
field16 created_on,
field6 street_1,
field7 street_2,
field30 town,
field8 county,
replace(c.field31, 'United Kingdon', 'United Kingdom') country,
case
	when field31 = 'Wales' then 'GB'
	else cc.country_code
end country_code,
field9 post_code,
field10 main_tel,
field11 main_fax,
field32 website,
-- field12 business_area, --custom field
field13 business_description,
field14 staff_type_employed,
field15 typical_sought_skills,
[Candidates Action History] candidate_action_history
from dbo.[Company Database] c
left join dbo.[tbl_con] tbl on c.field46 = tbl.con_username
left join dbo.country_code cc on replace(c.field31, 'United Kingdon', 'United Kingdom') = cc.country_name
-- where c.field31 = 'Germany'
)

select 
company_id "company-externalId",
CASE
	WHEN rn <> 1 THEN concat(COALESCE(company_name, 'No company name'), char(32),rn)
	ELSE COALESCE(company_name, 'No company name')
END "company-name",
format(created_on, 'yyyy-MM-dd') created_on,
-- record_owner,
COALESCE(record_owner_email, '') "company-owners",
COALESCE(town, '') "company-locationCity",
COALESCE(county, '') "company-locationState",
CASE 
	WHEN country_code is not null THEN country_code 
	ELSE 'GB'
END "company-locationCountry",
COALESCE(
	CASE 
		WHEN street_1 is not null THEN 
			CASE 
				WHEN RIGHT(RTRIM(street_1), 1) IN (',', '.') THEN SUBSTRING(RTRIM(street_1),1,LEN(RTRIM(street_1)) - 1) ELSE street_1 
			END
	END,
	CASE 
		WHEN street_2 is not null THEN 
			CASE 
				WHEN RIGHT(RTRIM(street_2), 1) IN (',', '.') THEN SUBSTRING(RTRIM(street_2),1,LEN(RTRIM(street_2)) - 1) ELSE street_2
			END
	END,
	town,
	county,
	'United Kingdom'
) "company-locationName",
concat(
	CASE 
		WHEN street_1 is not null THEN 
			CASE 
				WHEN RIGHT(RTRIM(street_1), 1) IN (',', '.') THEN concat(SUBSTRING(RTRIM(street_1),1,LEN(RTRIM(street_1)) - 1), ', ') ELSE concat(street_1,', ') 
			END
	END,
	CASE 
		WHEN street_2 is not null THEN 
			CASE 
				WHEN RIGHT(RTRIM(street_2), 1) IN (',', '.') THEN concat(SUBSTRING(RTRIM(street_2),1,LEN(RTRIM(street_2)) - 1), ', ') ELSE concat(street_2,', ') 
			END
	END,
	CASE WHEN town is not null THEN concat(town, ', ') END,
	CASE WHEN county is not null THEN concat(county, ', ') END,
	CASE WHEN country is not null THEN country END
) "company-locationAddress",
COALESCE(post_code, '') "company-locationZipCode",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(main_tel))  <> 0 THEN [dbo].[extract_number_from_str](main_tel) ELSE COALESCE(RTRIM(main_tel), '')
END "company-phone",
CASE
	WHEN PATINDEX('%[A-Za-z]%', RTRIM(main_fax)) <> 0 THEN [dbo].[extract_number_from_str](main_fax) ELSE COALESCE(RTRIM(main_fax), '')
END "company-fax",
CASE
	WHEN len(CONVERT(NVARCHAR(MAX), website)) < 10 THEN ''
	WHEN CHARINDEX('#/www', website) <> 0 THEN replace(replace(LEFT(CONVERT(NVARCHAR(MAX), website), CHARINDEX('/',website,9)-1), '///', '//'), '#', '')
	WHEN CHARINDEX('#http://www.http', website) <> 0 THEN LEFT(CONVERT(NVARCHAR(MAX), website), CHARINDEX('#',website,9)-1)
	WHEN len(CONVERT(NVARCHAR(MAX), website)) > 100 THEN CASE
																													WHEN charindex('http://#https', website) <> 0 THEN replace(LEFT(CONVERT(NVARCHAR(MAX), website), CHARINDEX('/',website,18)-1), 'http://#', '')
																													WHEN charindex('#http', website) <> 0 THEN replace(LEFT(CONVERT(NVARCHAR(MAX), website), CHARINDEX('/',website,10)-1), '#', '')
																													ELSE LEFT(CONVERT(NVARCHAR(MAX), website), CHARINDEX('/',website,9)-1)
																											 END
	WHEN charindex('https', website) = 0 THEN substring(RTRIM(CONVERT(NVARCHAR(MAX), website)), charindex('http', CONVERT(NVARCHAR(MAX), website)), LEN(RTRIM(CONVERT(NVARCHAR(MAX), website))) - charindex('http', CONVERT(NVARCHAR(MAX), website)))
	ELSE substring(RTRIM(CONVERT(NVARCHAR(MAX), website)), charindex('https', CONVERT(NVARCHAR(MAX), website)), LEN(RTRIM(CONVERT(NVARCHAR(MAX), website))) - charindex('https', CONVERT(NVARCHAR(MAX), website)))
END "company-website",
concat(
	CASE
		WHEN sequence_number is not null then concat('Company Sequence Number: ', sequence_number)
	END,
	CASE 
		WHEN company_id is not null then concat(CHAR(10), 'External ID: ', company_id)
	END,
	CASE 
		WHEN staff_type_employed is not null then concat(CHAR(10), 'Staff type employed: ', staff_type_employed)
	END,
	CASE 
		WHEN typical_sought_skills is not null then concat(CHAR(10), 'Typical sought skills: ', typical_sought_skills)
	END,
	CASE
		WHEN business_description is not null then concat(CHAR(10), 'Description: ', business_description)
	END,
	CASE
		WHEN len(CONVERT(NVARCHAR(MAX), website)) > 100 THEN 
		concat(
			CHAR(10), 
			'Full website address: ',
			CASE
				WHEN charindex('https', website) = 0 THEN substring(RTRIM(CONVERT(NVARCHAR(MAX), website)), charindex('http', CONVERT(NVARCHAR(MAX), website)), LEN(RTRIM(CONVERT(NVARCHAR(MAX), website))) - charindex('http', CONVERT(NVARCHAR(MAX), website)))
				ELSE substring(RTRIM(CONVERT(NVARCHAR(MAX), website)), charindex('https', CONVERT(NVARCHAR(MAX), website)), LEN(RTRIM(CONVERT(NVARCHAR(MAX), website))) - charindex('https', CONVERT(NVARCHAR(MAX), website)))
			END
		)
	END
) "company-note"

INTO _01_company_sample
from company
