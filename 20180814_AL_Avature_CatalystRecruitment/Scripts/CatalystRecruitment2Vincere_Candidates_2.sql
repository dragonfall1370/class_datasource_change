USE CatalystRecruitment;

WITH
Person_Tags AS (
	SELECT
	t.contactId
	, STRING_AGG(NULLIF(t.tag,''), ',') AS tags
	FROM (
		SELECT
		pt.contactId, pt.tag
		FROM
		[dbo].[person] p
		LEFT JOIN [dbo].[person.tag] pt ON p.contactId = pt.contactId
	) t
	GROUP BY t.contactId
)
--Person_Tags AS (
--	SELECT 
--	p.contactId,
--	STUFF(
--		(SELECT ',' + pt.tag
--		FROM [dbo].[person.tag] pt
--		WHERE pt.contactId = p.contactId
--		ORDER BY pt.tag
--		FOR XML PATH('')),
--		1, 1, '') Tags
--	FROM [dbo].[person] p
--	GROUP BY p.contactId
--		--ORDER BY 1
--)
, Person_Addresses AS (
SELECT
	p.[contactId] AS contactId
	,aw.[contactId] AS awContactId
	,aw.[street] AS awStreet
	,aw.[city] AS awCity
	,aw.[state] AS awState
	,aw.[zipCode] AS awZipCode
	,aw.[country] AS awCountry
	,ah.[contactId] AS ahContactId
	,ah.[street] AS ahStreet
	,ah.[city] AS ahCity
	,ah.[state] AS ahState
	,ah.[zipCode] AS ahZipCode
	,ah.[country] AS ahCountry
	FROM
	[dbo].[person] p
	LEFT JOIN [dbo].[address.work] aw ON p.contactId = aw.contactId
	LEFT JOIN [dbo].[address.home] ah ON p.contactId = ah.contactId
	LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 0
)
--SELECT * FROM Person_Addresses
, Person_Phones AS (
SELECT
	p.[contactId] AS contactId
	,pm.[contactId] AS pmContactId
	,COALESCE(pm.phoneNumber, '') + COALESCE(pm.extension, '') AS PhoneMobile
	,pw.[contactId] AS pwContactId
	,COALESCE(pw.phoneNumber, '') + COALESCE(pw.extension, '') AS PhoneWork
	,ph.[contactId] AS phContactId
	,COALESCE(ph.phoneNumber, '') + COALESCE(ph.extension, '') AS PhoneHome
	,pf.[contactId] AS pfContactId
	,COALESCE(pf.phoneNumber, '') + COALESCE(pf.extension, '') AS PhoneFax
	FROM
	[dbo].[person] p
	LEFT JOIN [dbo].[phone.mobile] pm ON p.contactId = pm.contactId
	LEFT JOIN [dbo].[phone.work] pw ON p.contactId = pw.contactId
	LEFT JOIN [dbo].[phone.home] ph ON p.contactId = ph.contactId
	LEFT JOIN [dbo].[phone.fax] pf ON p.contactId = pf.contactId
	LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 0
)

, Person_Phones_Flatened as (select p.contactId
    , string_agg(nullif(PhoneMobile,''),',') as MobilePhone
    , string_agg(nullif(PhoneWork,''),',') as WorkPhone
    , string_agg(nullif(PhoneHome,''),',') as HomePhone
    , string_agg(nullif(PhoneFax,''),',') as Fax
    from Person_Phones p
    group by p.contactId)

--, Person_Phones_Flatened AS (
--	SELECT 
--	contactId,
--	STUFF((SELECT ',' + tpd1.PhoneWork
--			FROM Person_Phones tpd1
--			WHERE tpd1.contactId = tpd.contactId
--			ORDER BY tpd1.PhoneWork
--			FOR XML PATH('')), 1, 1, '') AS [WorkPhone],
--	STUFF((SELECT ',' + tpd2.PhoneMobile
--			FROM Person_Phones tpd2
--			WHERE tpd2.contactId = tpd.contactId
--			ORDER BY tpd2.PhoneMobile
--			FOR XML PATH('')), 1, 1, '') AS [MobilePhone],
--	STUFF((SELECT ',' + tpd3.PhoneFax
--			FROM Person_Phones tpd3
--			WHERE tpd3.contactId = tpd.contactId
--			ORDER BY tpd3.PhoneFax
--			FOR XML PATH('')), 1, 1, '') AS [Fax],
--	STUFF((SELECT ',' + tpd4.PhoneHome
--			FROM Person_Phones tpd4
--			WHERE tpd4.contactId = tpd.contactId
--			ORDER BY tpd4.PhoneHome
--			FOR XML PATH('')), 1, 1, '') AS [HomePhone]
--		FROM Person_Phones tpd
--		GROUP BY tpd.contactId
--		--ORDER BY 1
--)
, Person_Emails AS (
SELECT
	p.[contactId] AS contactId
	,eh.[contactId] AS ehContactId
	,COALESCE(eh.emailAddress, '') AS EmailHome
	,eu.[contactId] AS euContactId
	,COALESCE(eu.emailAddress, '') AS EmailUndefinedType
	,ew.[contactId] AS ewContactId
	,COALESCE(ew.emailAddress, '') AS EmailWork
	FROM
	[dbo].[person] p
	LEFT JOIN [dbo].[email.home] eh ON p.contactId = eh.contactId
	LEFT JOIN [dbo].[email.undefinedType] eu ON p.contactId = eu.contactId
	LEFT JOIN [dbo].[email.work] ew ON p.contactId = ew.contactId
	LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
	WHERE (p.contactId = eh.contactId OR p.contactId = eu.contactId OR p.contactId = ew.contactId)
	AND (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 0
)

, Person_Emails_Flatened as (select p.contactId
    , string_agg(nullif(EmailHome,''),',') as EmailHome
    , string_agg(nullif(EmailUndefinedType,''),',') as EmailUndefinedType
    , string_agg(nullif(EmailWork,''),',') as EmailWork
    from Person_Emails p
    group by p.contactId)
--, Person_Emails_Flatened AS (
--	SELECT 
--	contactId,
--	STUFF((SELECT ',' + tpd1.EmailHome
--			FROM Person_Emails tpd1
--			WHERE tpd1.contactId = tpd.contactId
--			ORDER BY tpd1.EmailHome
--			FOR XML PATH('')), 1, 1, '') AS [EmailHome],
--	STUFF((SELECT ',' + tpd2.EmailUndefinedType
--			FROM Person_Emails tpd2
--			WHERE tpd2.contactId = tpd.contactId
--			ORDER BY tpd2.EmailUndefinedType
--			FOR XML PATH('')), 1, 1, '') AS [EmailUndefinedType],
--	STUFF((SELECT ',' + tpd3.EmailWork
--			FROM Person_Emails tpd3
--			WHERE tpd3.contactId = tpd.contactId
--			ORDER BY tpd3.EmailWork
--			FOR XML PATH('')), 1, 1, '') AS [EmailWork]
--		FROM Person_Emails tpd
--		GROUP BY tpd.contactId
--		--ORDER BY 1
--)

, Person_Notes AS (
	SELECT
	p.contactId,
	STUFF(
		COALESCE('Source: ' + NULLIF(p.source, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Tags: ' + NULLIF(pt.Tags, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Home Location Address: ' + NULLIF(pa.ahStreet, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Home Location State: ' + NULLIF(pa.ahState, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Home Location ZIP / Postal: ' + NULLIF(pa.ahZipCode, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Home Location Country: ' + NULLIF(pa.ahCountry, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Work Phone: ' + NULLIF(pp.WorkPhone, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Home Phone: ' + NULLIF(pp.HomePhone, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Fax: ' + NULLIF(pp.Fax, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Website: ' + NULLIF(wh.url, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Level of Expertise: ' + NULLIF(pn1._Level_of_Expertise_, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Often clients ask us for specific project exposure: ' + COALESCE(pn1._Often_clients_ask_us_for_specific_project_exposure, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('In which country are you currently located: ' + COALESCE(pn1._In_which_country_are_you_currently_located__,
		pn2._In_which_country_are_you_currently_located__, pn3._In_which_country_are_you_currently_located__, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('How quickly could you mobilize to New Zealand: ' + COALESCE(pn1._How_quickly_could_you_mobilize_to_New_Zealand__,
		pn2._How_quickly_could_you_mobilise_to_New_Zealand__, pn3._How_quickly_could_you_mobilise_to_New_Zealand__, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Where in New Zealand would you consider roles: ' + COALESCE(pn1._Where_in_New_Zealand_would_you_consider_roles__,
		pn2._Where_in_New_Zealand_would_you_consider_roles__, pn3._Where_in_New_Zealand_would_you_consider_roles__, '') + CHAR(13) + CHAR(10), '') +

		COALESCE('Specialisation: ' + NULLIF(pn2.Specialisation, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('In which type of environments have you worked: ' + NULLIF(pn2._In_which_type_of_environments_have_you_worked__, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('How many years of experience in Engineering Consultants: ' + NULLIF(pn2._How_many_years_of_experience_in_Engineering_Consultants__, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('How many years of experience in Local Authority: ' + NULLIF(pn2._How_many_years_of_experience_in_Local_Authority__, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('How many years of experience in Government Agency: ' + NULLIF(pn2._How_many_years_of_experience_in_Government_Agency__, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Level of Expertise: ' + NULLIF(pn2._Level_of_Expertise_, '') + CHAR(13) + CHAR(10), '') +
		--COALESCE('In which country are you currently located: ' + NULLIF(pn2._In_which_country_are_you_currently_located__, '') + CHAR(13) + CHAR(10), '') +
		--COALESCE('How quickly could you mobilise to New Zealand: ' + COALESCE(pn2._How_quickly_could_you_mobilise_to_New_Zealand__, '') + CHAR(13) + CHAR(10), '') +
		--COALESCE('Where in New Zealand would you consider roles: ' + NULLIF(pn2._Where_in_New_Zealand_would_you_consider_roles__, '') + CHAR(13) + CHAR(10), '') +

		COALESCE('Head Office Based: ' + NULLIF(pn3._Head_Office_Based_, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Operational or Site based: ' + NULLIF(pn3._Operational_or_Site_based_, '') + CHAR(13) + CHAR(10), '') +
		COALESCE('Often clients ask us for specific project exposure: ' + NULLIF(pn3._Often_clients_ask_us_for_specific_project_exposure, '') + CHAR(13) + CHAR(10), '') +
		--COALESCE('In which country are you currently located: ' + NULLIF(pn3._In_which_country_are_you_currently_located__, '') + CHAR(13) + CHAR(10), '') +
		--COALESCE('How quickly could you mobilise to New Zealand: ' + COALESCE(pn3._How_quickly_could_you_mobilise_to_New_Zealand__, '') + CHAR(13) + CHAR(10), '') +
		--COALESCE('Where in New Zealand would you consider roles: ' + NULLIF(pn3._Where_in_New_Zealand_would_you_consider_roles__, '') + CHAR(13) + CHAR(10), '') +
		
		COALESCE('Reason for candidate pulling out of Catalyst referral process: ' + NULLIF(pn4._Reason_for_candidate_pulling_out_of_Catalyst_referral_process_, '') + CHAR(13) + CHAR(10), '')
		, 1, 0, '') as Notes
	FROM
	[dbo].[person] p
	LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
	LEFt JOIN Person_Emails_Flatened pe ON p.contactId = pe.contactId
	LEFT JOIN Person_Phones_Flatened pp ON p.contactId = pp.contactId
	LEFT JOIN Person_Addresses pa ON p.contactId = pa.contactId
	LEFT JOIN [dbo].[website.home] wh ON p.contactId = wh.contactId
	LEFT JOIN [dbo].[Persons.CVUpdatePortalStage2ProfessionalProjectManagementID233] pn1 ON p.contactId = pn1._Person_ID_
	LEFT JOIN [dbo].[Persons.CVUpdatePortalStage2EngineeringLocalAuthorityGovernmentID215] pn2 ON p.contactId = pn2._Person_ID_
	LEFT JOIN [dbo].[Persons.CVUpdatePortalStage2ConstructionID213] pn3 ON p.contactId = pn3._Person_ID_
	LEFT JOIN [dbo].[Persons.CandidateNotesReasonsforpullingoutofreferralprocessID210] pn4 ON p.contactId = pn4._Person_ID_
	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 0
)

, Person_Attachments AS (
	SELECT 
	p.contactId,
	STUFF(
		(SELECT ',' + REPLACE(RIGHT(pa.fileName, len(pa.fileName) - 3), ',', '_')
		FROM [dbo].[person.attachment] pa
		WHERE pa.contactId = p.contactId
		ORDER BY REPLACE(RIGHT(pa.fileName, len(pa.fileName) - 3), ',', '_')
		FOR XML PATH('')),
		1, 1, '') Documents
	FROM [dbo].[person] p
	GROUP BY p.contactId
		--ORDER BY 1
)

SELECT
--TOP 10
p.contactId AS [candidate-externalId]
, CASE WHEN (LTRIM(REPLACE(p.firstName,'?','')) = '' OR  p.firstName IS NULL) THEN 'FirstName' ELSE LTRIM(REPLACE(p.firstName,'?','')) END AS [candidate-firstName]
, CASE WHEN (LTRIM(REPLACE(p.lastName,'?','')) = '' OR  p.lastName IS NULL) THEN 'LastName' ELSE LTRIM(REPLACE(p.lastName,'?','')) END AS [candidate-Lastname]
, COALESCE(p.jobTitle, '') AS [candidate-jobTitle1]
, IIF(pa.awContactId IS NOT NULL, COALESCE(pa.awStreet, ''), COALESCE(pa.ahStreet, '')) AS [candidate-address]
, IIF(pa.awContactId IS NOT NULL, COALESCE(pa.awState, ''), COALESCE(pa.ahState, '')) AS [candidate-State]
, IIF(pa.awContactId IS NOT NULL, COALESCE(pa.awZipCode, ''), COALESCE(pa.ahZipCode, '')) AS [candidate-zipCode]
, COALESCE((SELECT TOP(1) [Code] FROM [dbo].[VincereCountryCodeDic] vcc WHERE UPPER(IIF(pa.awContactId IS NOT NULL, COALESCE(pa.awCountry, ''), COALESCE(pa.ahCountry, ''))) LIKE UPPER(vcc.[Name])), '') AS [candidate-Country]
, IIF(TRIM(',' FROM TRIM(COALESCE(pe.EmailHome, pe.EmailUndefinedType, ''))) = '', CAST(NEWID() AS VARCHAR(50)) + '@email.com', TRIM(',' FROM TRIM(COALESCE(pe.EmailHome, pe.EmailUndefinedType, pe.EmailWork, '')))) AS [candidate-email]
, TRIM(',' FROM TRIM(COALESCE(pe.EmailWork, ''))) AS [candidate-workEmail]
, TRIM(',' FROM COALESCE(pp.MobilePhone, pp.WorkPhone, pp.HomePhone, '')) AS [candidate-phone]
, TRIM(',' FROM COALESCE(pp.MobilePhone, '')) AS [candidate-mobile]
, TRIM(',' FROM COALESCE(pp.WorkPhone, '')) AS [candidate-workPhone]
, TRIM(',' FROM COALESCE(pp.HomePhone, '')) AS [candidate-homePhone]
, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(pn.Notes, ''),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') AS [candidate-note]
, TRIM(',' FROM COALESCE(pat.Documents, '')) AS [candidate-resume]
--, COALESCE(pcf._Skype_Name_, '') AS [candidate-skype]
--, COALESCE(pcf._Current_Salary_, '') AS [candidate-currentSalary]
--, COALESCE(pcf._Desired_Salary_, '') AS [candidate-desiredSalary]

FROM
[dbo].[person] p
LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
LEFt JOIN Person_Emails_Flatened pe ON p.contactId = pe.contactId
LEFT JOIN Person_Addresses pa ON p.contactId = pa.contactId
LEFT JOIN Person_Phones_Flatened pp ON p.contactId = pp.contactId
LEFT JOIN Person_Notes pn ON p.contactId = pn.contactId
LEFT JOIN Person_Attachments pat ON p.contactId = pat.contactId
LEFT JOIN [dbo].[PersonsCustomFields] pcf ON p.contactId = pcf._Person_ID_
WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 0