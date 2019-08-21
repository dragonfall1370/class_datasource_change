USE CatalystRecruitment;

-- Preparing default data

IF((SELECT COUNT(*) FROM [dbo].[company] c WHERE c.firstName = 'DEFAULT COMPANY') = 0)
	BEGIN
		INSERT INTO [dbo].[company]
		VALUES(1111111111, 'company','DEFAULT COMPANY', NULL, GETDATE(), (SELECT TOP 1 appUserId FROM [dbo].[User]), NULL, NULL, NULL, 'client')
	END

GO

--UPDATE [dbo].[company]
--SET [contactType] = 'company'
--	,[firstName] = 'DEFAULT COMPANY'
--	,[lastName] = NULL
--	,[creationTime] = GETDATE()
--	,[appUserIdCreator] = (SELECT TOP 1 appUserId FROM [dbo].[User])
--	,[jobTitle] = NULL
--	,[company] = NULL
--	,[source] = NULL
--	,[tags] = 'client'
--WHERE [contactId] = 1111111111

IF((SELECT COUNT(*) FROM [dbo].[address.work] aw WHERE aw.contactId = 1111111111) = 0)
BEGIN
	INSERT INTO [dbo].[address.work]
	VALUES(1111111111, '26 Patey Street', 'Newmarket', 'Auckland', 'PO Box 9848', 'New Zealand')
END

GO

--UPDATE [dbo].[address.work]
--SET [street] = '26 Patey Street'
--    ,[city] = 'Newmarket'
--    ,[state] = 'Auckland'
--    ,[zipCode] = 'PO Box 9848'
--    ,[country] = 'New Zealand'
--WHERE [contactId] = 1111111111

IF((SELECT COUNT(*) FROM [dbo].[website.home] wh WHERE wh.contactId = 1111111111) = 0)
BEGIN
	INSERT INTO [dbo].[website.home]
	VALUES(1111111111, 'http://default-comp-website.com')
END

GO

IF((SELECT COUNT(*) FROM [dbo].[phone.work] pw WHERE pw.contactId = 1111111111) = 0)
BEGIN
	INSERT INTO [dbo].[phone.work]
	VALUES(1111111111, '11 111 1111', NULL)
END

GO

IF((SELECT COUNT(*) FROM [dbo].[company.tag] ct WHERE ct.contactId = 1111111111) = 0)
BEGIN
	INSERT INTO [dbo].[company.tag]
	VALUES(1111111111, 'company', 'client')
END

GO

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
	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, ',') WHERE UPPER(VALUE) = UPPER('client'))  = 0
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
--, PersonPhones AS (
--	SELECT
--	pid.contactId
--	, pw.phoneNumber AS PhoneWork
--	, ph.phoneNumber AS PhoneHome
--	, pm.phoneNumber AS PhoneMobile
--	, pm.phoneNumber AS PhoneFax
--	FROM
--	PersonIndexs pid
--	LEFT JOIN PhoneWork pw ON pid.contactId = pw.contactId
--	LEFT JOIN PhoneHome ph ON pid.contactId = ph.contactId
--	LEFT JOIN PhoneMobile pm ON pid.contactId = pm.contactId
--	LEFT JOIN PhoneFax pf ON pid.contactId = pf.contactId
--)

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
	STRING_AGG(url, ',') AS url
	FROM (	
		SELECT contactId, url, ROW_NUMBER() OVER(partition by url Order by contactId) As rowNum
		FROM [dbo].[website.home]
	) wh
	WHERE wh.rowNum = 1
	GROUP BY contactId
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
--SELECT * FROM PersonPhones
--SELECT * FROM AddressHome
--SELECT * FROM AddressWork

-- main script --

SELECT --TOP(3)
c.contactId AS [company-externalId],
--IIF(c.contactId IN (SELECT [contactId] FROM CompDup WHERE cdt.RN > 1), CONCAT(COALESCE(cdt.ComName, ''),' ', COALESCE(cdt.RN, '')), iif(c.firstName = '' OR c.firstName IS NULL, 'No CompanyName', c.firstName)) AS [company-name],
c.firstName AS [company-name],
COALESCE(aw.street, ah.street, '') AS [company-locationAddress],
COALESCE(aw.city, ah.city, '') AS [company-locationCity],
COALESCE(aw.state, ah.state, '') AS [company-locationState],
COALESCE(aw.zipCode, ah.zipCode, '') AS [company-locationZipCode],
COALESCE((SELECT TOP(1) [Code] FROM [dbo].[VincereCountryCodeDic] vcc WHERE UPPER(COALESCE(aw.country, ah.country, '')) LIKE UPPER(vcc.[Name])), '') AS [company-locationCountry],
COALESCE(LEFT(wh.url, 100), '') AS [company-website], --limitted by 100 characters
COALESCE(pw.phoneNumber, ph.phoneNumber, pf.phoneNumber, pm.phoneNumber, '') AS [company-phone],
COALESCE('Tags: ' + NULLIF(ct.tags, '') + @NewLineChar, '') +
COALESCE('Home Location Address: ' + NULLIF(ah.street, '') + @NewLineChar, '') +
COALESCE('Home Location City: ' + NULLIF(ah.city, '') + @NewLineChar, '') +
COALESCE('Home Location State: ' + NULLIF(ah.state, '') + @NewLineChar, '') +
COALESCE('Home Location ZIP / Postal: ' + NULLIF(ah.zipCode, '') + @NewLineChar, '') +
COALESCE('Home Location Country: ' + NULLIF(ah.country, '') + @NewLineChar, '') +
COALESCE('Mobile Phone: ' + NULLIF(pm.phoneNumber, '') + @NewLineChar, '') +
COALESCE('Fax: ' + NULLIF(pf.phoneNumber, '') + @NewLineChar, '') +
COALESCE('Home Phone: ' + NULLIF(ph.phoneNumber, '') + @NewLineChar, '')
AS [company-note]
FROM
[dbo].[company] c
LEFT JOIN CompanyTags ct ON c.contactId = ct.contactId
LEFT JOIN WebHome wh ON c.contactId = wh.contactId
LEFT JOIN AddressWork aw ON c.contactId = aw.contactId
LEFT JOIN AddressHome ah ON c.contactId = ah.contactId
LEFT JOIN PhoneWork pw ON c.contactId = pw.contactId
LEFT JOIN PhoneHome ph ON c.contactId = ph.contactId
LEFT JOIN PhoneMobile pm ON c.contactId = pm.contactId
LEFT JOIN PhoneFax pf ON c.contactId = pf.contactId