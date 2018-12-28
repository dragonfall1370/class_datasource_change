USE CatalystRecruitment

-- utilities
DECLARE @NewLineChar AS CHAR(2) = CHAR(13) + CHAR(10);
WITH
CompanyTags AS (
	SELECT
	[contactId],
	STRING_AGG(ct.tag, ',') tags
	FROM
	[dbo].[company.tag] ct
	GROUP BY [contactId]
)

,PersonTags AS (
	SELECT
	[contactId],
	STRING_AGG(pt.tag, ',') tags
	FROM
	[dbo].[person.tag] pt
	GROUP BY [contactId]
)

, PersonIndexs AS (
	SELECT
	contactId
	FROM PersonTags
	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, ',') WHERE UPPER(VALUE) = UPPER('client'))  = 1
)

, EmailHome AS (
	SELECT
	contactId,
	STRING_AGG([dbo].ufn_PopulateEmailAddress(emailAddress, contactId, RowNum), ',') AS emailAddress
	FROM (	
		SELECT contactId, emailAddress, ROW_NUMBER() OVER(partition by emailAddress Order by contactId) As RowNum
		FROM [dbo].[email.home]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)
--SELECT * FROM EmailHome
, EmailUndefinedType AS (
	SELECT
	contactId,
	STRING_AGG([dbo].ufn_PopulateEmailAddress(emailAddress, contactId, RowNum), ',') AS emailAddress
	FROM (	
		SELECT contactId, emailAddress, ROW_NUMBER() OVER(partition by emailAddress Order by contactId) As RowNum
		FROM [dbo].[email.undefinedType]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)
--SELECT * FROM EmailUndefinedType
, EmailWork AS (
	SELECT
	contactId,
	STRING_AGG([dbo].ufn_PopulateEmailAddress(emailAddress, contactId, RowNum), ',') AS emailAddress
	FROM (	
		SELECT contactId, emailAddress, ROW_NUMBER() OVER(partition by emailAddress Order by contactId) As RowNum
		FROM [dbo].[email.work]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)

--, PersonEmails AS (
--	SELECT
--	pid.contactId
--	, eh.emailAddress AS EmailHome
--	, eu.emailAddress AS EmailUndefinedType
--	, ew.emailAddress AS EmailWork
--	FROM
--	PersonIndexs pid
--	LEFT JOIN EmailHome eh ON pid.contactId = eh.contactId
--	LEFT JOIN EmailUndefinedType eu ON pid.contactId = eu.contactId
--	LEFT JOIN EmailWork ew ON pid.contactId = ew.contactId
--)

, PhoneWork AS (
	SELECT
	contactId,
	STRING_AGG(phoneNumber + extension, ',') AS phoneNumber
	FROM (	
		SELECT contactId, phoneNumber, extension, ROW_NUMBER() OVER(partition by phoneNumber + extension Order by contactId) As RowNum
		FROM [dbo].[phone.work]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)

, PhoneHome AS (
	SELECT
	contactId,
	STRING_AGG(phoneNumber + extension, ',') AS phoneNumber
	FROM (	
		SELECT contactId, phoneNumber, extension, ROW_NUMBER() OVER(partition by phoneNumber + extension Order by contactId) As RowNum
		FROM [dbo].[phone.home]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)

, PhoneMobile AS (
	SELECT
	contactId,
	STRING_AGG(phoneNumber + extension, ',') AS phoneNumber
	FROM (	
		SELECT contactId, phoneNumber, extension, ROW_NUMBER() OVER(partition by phoneNumber + extension Order by contactId) As RowNum
		FROM [dbo].[phone.mobile]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)

, PhoneFax AS (
	SELECT
	contactId,
	STRING_AGG(phoneNumber + extension, ',') AS phoneNumber
	FROM (	
		SELECT contactId, phoneNumber, extension, ROW_NUMBER() OVER(partition by phoneNumber + extension Order by contactId) As RowNum
		FROM [dbo].[phone.fax]
	) ea
	WHERE ea.RowNum = 1
	GROUP BY contactId
)
, PersonPhones AS (
	SELECT
	pid.contactId
	, pw.phoneNumber AS PhoneWork
	, ph.phoneNumber AS PhoneHome
	, pm.phoneNumber AS PhoneMobile
	, pm.phoneNumber AS PhoneFax
	FROM
	PersonIndexs pid
	LEFT JOIN PhoneWork pw ON pid.contactId = pw.contactId
	LEFT JOIN PhoneHome ph ON pid.contactId = ph.contactId
	LEFT JOIN PhoneMobile pm ON pid.contactId = pm.contactId
	LEFT JOIN PhoneFax pf ON pid.contactId = pf.contactId
)

, AddressHome AS (
	SELECT a.*
	FROM
	[dbo].[address.home] a
	INNER JOIN
	(SELECT contactId, MAX([dbo].ufn_EvaluateAddress(street, city, state, zipCode, country)) AS addPoint
	FROM [dbo].[address.home]
	GROUP BY contactId) ag
	ON a.contactId = ag.contactId
	AND [dbo].ufn_EvaluateAddress(a.street, a.city, a.state, a.zipCode, a.country) = ag.addPoint
)

, AddressWork AS (
	SELECT a.*
	FROM
	[dbo].[address.work] a
	INNER JOIN
	(SELECT contactId, MAX([dbo].ufn_EvaluateAddress(street, city, state, zipCode, country)) AS addPoint
	FROM [dbo].[address.work]
	GROUP BY contactId) ag
	ON a.contactId = ag.contactId
	AND [dbo].ufn_EvaluateAddress(a.street, a.city, a.state, a.zipCode, a.country) = ag.addPoint
)

, CompDup AS (
	SELECT c.contactId, c.firstName AS compName, ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(c.firstName)) ORDER BY c.contactId ASC) AS RN
	FROM [dbo].[company] c
)

, WebHome AS (
	SELECT
	contactId,
	STRING_AGG(NULLIF(url, ''), ',') AS url
	FROM (	
		SELECT contactId, url, ROW_NUMBER() OVER(partition by url Order by contactId) As rowNum
		FROM [dbo].[website.home]
	) wh
	WHERE wh.rowNum = 1
	GROUP BY contactId
)

, PersonAttachments AS (
	SELECT
	pa.contactId
	, STRING_AGG(NULLIF(pa.[fileName], ''), ',') AS docs
	FROM
	[dbo].[person.attachment] pa
	GROUP BY pa.contactId
)
-- testing
--SELECT * FROM ContactIndexs
-- 1160
-- 7s - 11s - 8s
--SELECT * FROM CandidateIndexs
-- 41804
-- 8s - 8s - 10s

--SELECT * FROM EmailHome
--SELECT * FROM EmailUndefinedType
--SELECT * FROM EmailWork
--SELECT * FROM PersonEmails
SELECT * FROM PersonPhones
WHERE PhoneWork IS NOT NULL OR PhoneHome IS NOT NULL OR PhoneMobile IS NOT NULL OR PhoneFax IS NOT NULL
--SELECT * FROM AddressHome
--SELECT * FROM AddressWork

-- main script --

SELECT --TOP(3)
COALESCE(co.contactId, '1111111111') AS [contact-companyId]
, c.contactId AS [contact-externalId]
, CASE WHEN (LTRIM(REPLACE(c.firstName,'?','')) = '' OR  c.firstName IS NULL) THEN 'FirstName' ELSE LTRIM(REPLACE(c.firstName,'?','')) END AS [contact-firstName]
, CASE WHEN (LTRIM(REPLACE(c.lastName,'?','')) = '' OR  c.lastName IS NULL) THEN 'LastName' ELSE LTRIM(REPLACE(c.lastName,'?','')) END AS [contact-lastName]
, COALESCE(c.jobTitle, '') AS [contact-jobTitle]
--, COALESCE(aw.street, '') AS [contact-locationAddress]
--, COALESCE(aw.state, '') AS [contact-locationState]
--, COALESCE(aw.zipCode, '') AS [contact-locationZipCode]
--, COALESCE((SELECT TOP(1) [Code] FROM [dbo].[VincereCountryCodeDic] vcc WHERE UPPER(COALESCE(aw.country, '')) LIKE UPPER(vcc.[Name])), '') AS [contact-locationCountry]
, COALESCE(eh.emailAddress, eu.emailAddress, ew.emailAddress, '') AS [contact-email]
, COALESCE(pm.phoneNumber, ph.phoneNumber, pw.phoneNumber, '') AS [contact-phone]
, COALESCE('Source: ' + NULLIF(c.source, '') + @NewLineChar, '') +
COALESCE('Tags: ' + NULLIF(pt.Tags, '') + @NewLineChar, '') +
COALESCE('Home Location Address: ' + NULLIF(ah.street, '') + @NewLineChar, '') +
COALESCE('Home Location City: ' + NULLIF(ah.city, '') + @NewLineChar, '') +
COALESCE('Home Location State: ' + NULLIF(ah.state, '') + @NewLineChar, '') +
COALESCE('Home Location ZIP / Postal: ' + NULLIF(ah.zipCode, '') + @NewLineChar, '') +
COALESCE('Home Location Country: ' + NULLIF(ah.country, '') + @NewLineChar, '') +
COALESCE('Work Phone: ' + NULLIF(pw.phoneNumber, '') + @NewLineChar, '') +
COALESCE('Home Phone: ' + NULLIF(ph.phoneNumber, '') + @NewLineChar, '') +
COALESCE('Fax: ' + NULLIF(pf.phoneNumber, '') + @NewLineChar, '') +
COALESCE('Website: ' + NULLIF(wh.url, '') + @NewLineChar, '') +
COALESCE('Reg Flag Or Important Notes: ' + NULLIF(pfc._Red_flags_or_important_notes_, '') + @NewLineChar, '') +
COALESCE('Extra Information: ' + NULLIF(pei._Extra_Information_, '') + @NewLineChar, '') +
COALESCE('Where Did You Hear About Us: ' + NULLIF(pei._Where_did_you_hear_about_us__, '') + @NewLineChar, '') AS [contact-note]
, COALESCE(pa.docs, '') AS [contact-document]
FROM
PersonIndexs pid
LEFT JOIN [dbo].[person] c ON pid.contactId = c.contactId
LEFT JOIN [dbo].[company] co ON c.company = co.firstName
LEFT JOIN PersonTags pt ON c.contactId = pt.contactId
LEFT JOIN WebHome wh ON c.contactId = wh.contactId
LEFT JOIN EmailHome eh ON c.contactId = eh.contactId
LEFT JOIN EmailUndefinedType eu ON c.contactId = eu.contactId
LEFT JOIN EmailWork ew ON c.contactId = ew.contactId
LEFT JOIN AddressWork aw ON c.contactId = aw.contactId
LEFT JOIN AddressHome ah ON c.contactId = ah.contactId
LEFT JOIN PhoneWork pw ON c.contactId = pw.contactId
LEFT JOIN PhoneHome ph ON c.contactId = ph.contactId
LEFT JOIN PhoneMobile pm ON c.contactId = pm.contactId
LEFT JOIN PhoneFax pf ON c.contactId = pf.contactId
LEFT JOIN PersonAttachments pa ON c.contactId = pa.contactId
LEFT JOIN [dbo].[Persons.FirstConversationnotesID206] pfc ON c.contactId = pfc._Person_ID_
LEFT JOIN [dbo].[Persons.ExtraInformationID205] pei ON c.contactId = pei._Person_ID_