USE CatalystRecruitment;

WITH
Person_Tags AS (
	SELECT 
	p.contactId,
	STUFF(
		(SELECT ',' + pt.tag
		FROM [dbo].[person.tag] pt
		WHERE pt.contactId = p.contactId
		ORDER BY pt.tag
		FOR XML PATH('')),
		1, 1, '') Tags
	FROM [dbo].[person] p
	GROUP BY p.contactId
		--ORDER BY 1
)
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
)
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
	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 1
)
, Person_Phones_Flatened AS (
	SELECT 
	contactId,
	STUFF((SELECT ',' + tpd1.PhoneWork
			FROM Person_Phones tpd1
			WHERE tpd1.contactId = tpd.contactId
			ORDER BY tpd1.PhoneWork
			FOR XML PATH('')), 1, 1, '') AS [WorkPhone],
	STUFF((SELECT ',' + tpd2.PhoneMobile
			FROM Person_Phones tpd2
			WHERE tpd2.contactId = tpd.contactId
			ORDER BY tpd2.PhoneMobile
			FOR XML PATH('')), 1, 1, '') AS [MobilePhone],
	STUFF((SELECT ',' + tpd3.PhoneFax
			FROM Person_Phones tpd3
			WHERE tpd3.contactId = tpd.contactId
			ORDER BY tpd3.PhoneFax
			FOR XML PATH('')), 1, 1, '') AS [Fax],
	STUFF((SELECT ',' + tpd4.PhoneHome
			FROM Person_Phones tpd4
			WHERE tpd4.contactId = tpd.contactId
			ORDER BY tpd4.PhoneHome
			FOR XML PATH('')), 1, 1, '') AS [HomePhone]
		FROM Person_Phones tpd
		GROUP BY tpd.contactId
		--ORDER BY 1
)
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
	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 1
)
, Person_Emails_Flatened AS (
	SELECT 
	contactId,
	STUFF((SELECT ',' + tpd1.EmailHome
			FROM Person_Emails tpd1
			WHERE tpd1.contactId = tpd.contactId
			ORDER BY tpd1.EmailHome
			FOR XML PATH('')), 1, 1, '') AS [EmailHome],
	STUFF((SELECT ',' + tpd2.EmailUndefinedType
			FROM Person_Emails tpd2
			WHERE tpd2.contactId = tpd.contactId
			ORDER BY tpd2.EmailUndefinedType
			FOR XML PATH('')), 1, 1, '') AS [EmailUndefinedType],
	STUFF((SELECT ',' + tpd3.EmailWork
			FROM Person_Emails tpd3
			WHERE tpd3.contactId = tpd.contactId
			ORDER BY tpd3.EmailWork
			FOR XML PATH('')), 1, 1, '') AS [EmailWork]
		FROM Person_Emails tpd
		GROUP BY tpd.contactId
		--ORDER BY 1
)
, Person_Notes AS (
	SELECT
	p.contactId,
	STUFF(
		COALESCE('Source: ' + NULLIF(p.source, '') + CHAR(10), '') +
		COALESCE('Tags: ' + NULLIF(pt.Tags, '') + CHAR(10), '') +
		COALESCE('Home Location Address: ' + NULLIF(pa.ahStreet, '') + CHAR(10), '') +
		COALESCE('Home Location State: ' + NULLIF(pa.ahState, '') + CHAR(10), '') +
		COALESCE('Home Location ZIP / Postal: ' + NULLIF(pa.ahZipCode, '') + CHAR(10), '') +
		COALESCE('Home Location Country: ' + NULLIF(pa.ahCountry, '') + CHAR(10), '') +
		COALESCE('Work Phone: ' + NULLIF(pp.WorkPhone, '') + CHAR(10), '') +
		COALESCE('Home Phone: ' + NULLIF(pp.HomePhone, '') + CHAR(10), '') +
		COALESCE('Fax: ' + NULLIF(pp.Fax, '') + CHAR(10), '') +
		COALESCE('Website: ' + NULLIF(wh.url, '') + CHAR(10), '') +
		COALESCE('Reg Flag Or Important Notes: ' + NULLIF(pfc._Red_flags_or_important_notes_, '') + CHAR(10), '') +
		COALESCE('Extra Information: ' + NULLIF(pei._Extra_Information_, '') + CHAR(10), '') +
		COALESCE('Where Did You Hear About Us: ' + NULLIF(pei._Where_did_you_hear_about_us__, '') + CHAR(10), '')
		, 1, 0, '') as Notes
	FROM
	[dbo].[person] p
	LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
	LEFt JOIN Person_Emails_Flatened pe ON p.contactId = pe.contactId
	LEFT JOIN Person_Phones_Flatened pp ON p.contactId = pp.contactId
	LEFT JOIN Person_Addresses pa ON p.contactId = pa.contactId
	LEFT JOIN [dbo].[website.home] wh ON p.contactId = wh.contactId
	LEFT JOIN [dbo].[Persons.FirstConversationnotesID206] pfc ON p.contactId = pfc._Person_ID_
	LEFT JOIN [dbo].[Persons.ExtraInformationID205] pei ON p.contactId = pei._Person_ID_
	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 1
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
--pope,-tim-catalyst-stephen--aTs90N.pdf,gregory,-stephen-cv-may-11-3wTrnw.pdf
--, Person_Activities_Comment AS (
--	SELECT
--	p.contactId,
--	STUFF(
--		COALESCE('Reg Flag Or Important Notes: ' + NULLIF(pfc._Red_flags_or_important_notes_, '') + CHAR(10), '') +
--		COALESCE('Extra Information: ' + NULLIF(pei._Extra_Information_, '') + CHAR(10), '') +
--		COALESCE('Where Did You Hear About Us: ' + NULLIF(pei._Where_did_you_hear_about_us__, '') + CHAR(10), '')
--		, 1, 0, '') as Comments
--	FROM
--	[dbo].[person] p
--	LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
--	LEFT JOIN [dbo].[Persons.FirstConversationnotesID206] pfc ON p.contactId = pfc._Person_ID_
--	LEFT JOIN [dbo].[Persons.ExtraInformationID205] pei ON p.contactId = pei._Person_ID_
--	WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 1
--)
-- Main script
SELECT
--TOP(10)

COALESCE(c.contactId, '1111111111') AS [contact-companyId]
, p.contactId AS [contact-externalId]
, CASE WHEN (LTRIM(REPLACE(p.firstName,'?','')) = '' OR  p.firstName IS NULL) THEN 'FirstName' ELSE LTRIM(REPLACE(p.firstName,'?','')) END AS [contact-firstName]
, CASE WHEN (LTRIM(REPLACE(p.lastName,'?','')) = '' OR  p.lastName IS NULL) THEN 'LastName' ELSE LTRIM(REPLACE(p.lastName,'?','')) END AS [contact-lastName]
, p.jobTitle AS [contact-jobTitle]
--, COALESCE(aw.street, '') AS [contact-locationAddress]
--, COALESCE(aw.state, '') AS [contact-locationState]
--, COALESCE(aw.zipCode, '') AS [contact-locationZipCode]
--, COALESCE((SELECT TOP(1) [Code] FROM [dbo].[VincereCountryCodeDic] vcc WHERE UPPER(COALESCE(aw.country, '')) LIKE UPPER(vcc.[Name])), '') AS [contact-locationCountry]
, IIF(TRIM(',' FROM TRIM(COALESCE(pe.EmailHome, pe.EmailUndefinedType, pe.EmailWork, ''))) = '', CAST(NEWID() AS VARCHAR(50)) + '@email.com', TRIM(',' FROM TRIM(COALESCE(pe.EmailHome, pe.EmailUndefinedType, pe.EmailWork, '')))) AS [contact-email]
, COALESCE(pp.MobilePhone, pp.WorkPhone, pp.HomePhone, '') AS [contact-phone]
, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(pn.Notes, ''),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') AS [contact-note]
, COALESCE(pa.Documents, '') AS [contact-document]
--,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(pac.Comments, ''),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') AS [contact-comment]
FROM [dbo].[person] p
LEFT JOIN [dbo].[company] c ON p.company = c.firstName
--LEFT JOIN [dbo].[address.work] aw ON p.contactId = aw.contactId
LEFT JOIN Person_Tags pt ON p.contactId = pt.contactId
LEFt JOIN Person_Emails_Flatened pe ON p.contactId = pe.contactId
LEFT JOIN Person_Phones_Flatened pp ON p.contactId = pp.contactId
LEFT JOIN Person_Notes pn ON p.contactId = pn.contactId
LEFT JOIN Person_Attachments pa ON p.contactId = pa.contactId
--LEFT JOIN Person_Activities_Comment pac ON p.contactId = pac.contactId
WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 1