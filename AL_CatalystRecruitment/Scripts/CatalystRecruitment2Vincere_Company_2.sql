USE CatalystRecruitment;

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

--company

WITH
  Com_Dup AS (
	SELECT c.contactId, c.firstName AS ComName, ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(c.firstName)) ORDER BY c.contactId ASC) AS RN
	FROM [dbo].[company] c
)
, AddWorkCheck AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN
	FROM [dbo].[address.work] o
)
--, AddWork AS (
--	SELECT 
--	pw.contactId,
--	STUFF((SELECT ',' + pw1.phoneNumber + pw1.extension
--			FROM [dbo].[phone.work] pw1
--			WHERE pw1.contactId = pw.contactId
--			ORDER BY pw1.phoneNumber + pw1.extension
--			FOR XML PATH('')), 1, 1, '') AS [workPhone]
--		FROM [dbo].[phone.work] pw
--		GROUP BY pw.contactId
--)
-- company with multi website.home check
, WebHomeCheck AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN
	FROM [dbo].[website.home] o
)
, WebHome AS (
	SELECT 
	wh.contactId,
	STUFF((SELECT ',' + wh1.url
			FROM [dbo].[website.home] wh1
			WHERE wh1.contactId = wh.contactId
			ORDER BY wh1.url
			FOR XML PATH('')), 1, 1, '') AS [url]
		FROM [dbo].[website.home] wh
		GROUP BY wh.contactId
)
-- company with multi address.home check
, AddHomeCheck AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN
	FROM [dbo].[address.home] o
)
-- create temp phones table
, Tmp_Phones_Dup AS (
	SELECT DISTINCT
	c.contactId [cid],
	pw.phoneNumber + IIF(pw.extension IS NOT NULL, 'ext' + pw.extension, '') AS [WorkPhone],
	pm.phoneNumber + IIF(pm.extension IS NOT NULL, 'ext' + pm.extension, '') AS [MobilePhone],
	pf.phoneNumber + IIF(pf.extension IS NOT NULL, 'ext' + pf.extension, '') AS [Fax],
	ph.phoneNumber + IIF(ph.extension IS NOT NULL, 'ext' + ph.extension, '') AS [HomePhone]
	FROM [dbo].[company] c
	LEFT JOIN [dbo].[phone.work] pw ON c.contactId = pw.contactId
	LEFT JOIN [dbo].[phone.mobile] pm ON c.contactId = pm.contactId
	LEFT JOIN [dbo].[phone.fax] pf ON c.contactId = pf.contactId
	LEFT JOIN[dbo].[phone.home] ph ON c.contactId = ph.contactId
	--ORDER BY [cid]
)
, Tmp_Phones AS (
	SELECT 
	tpd.cid,
	STUFF((SELECT ',' + tpd1.WorkPhone
			FROM Tmp_Phones_Dup tpd1
			WHERE tpd1.cid = tpd.cid
			ORDER BY tpd1.WorkPhone
			FOR XML PATH('')), 1, 1, '') AS [WorkPhone],
	STUFF((SELECT ',' + tpd2.MobilePhone
			FROM Tmp_Phones_Dup tpd2
			WHERE tpd2.cid = tpd.cid
			ORDER BY tpd2.MobilePhone
			FOR XML PATH('')), 1, 1, '') AS [MobilePhone],
	STUFF((SELECT ',' + tpd3.Fax
			FROM Tmp_Phones_Dup tpd3
			WHERE tpd3.cid = tpd.cid
			ORDER BY tpd3.Fax
			FOR XML PATH('')), 1, 1, '') AS [Fax],
	STUFF((SELECT ',' + tpd4.HomePhone
			FROM Tmp_Phones_Dup tpd4
			WHERE tpd4.cid = tpd.cid
			ORDER BY tpd4.HomePhone
			FOR XML PATH('')), 1, 1, '') AS [HomePhone]
		FROM Tmp_Phones_Dup tpd
		GROUP BY tpd.cid
		--ORDER BY 1
)
, Com_Tags AS (
	SELECT 
	c.contactId,
	STUFF((SELECT ',' + ct.tag
			FROM [dbo].[company.tag] ct
			WHERE ct.contactId = c.contactId
			ORDER BY ct.tag
			FOR XML PATH('')), 1, 1, '') Tags
		FROM [company] c
		GROUP BY c.contactId
		--ORDER BY 1
)
, Tmp_Notes AS (
	SELECT
		c.contactId,
		STUFF(
			COALESCE('Tags: ' + NULLIF(t.Tags, '') + CHAR(10), '') +
			COALESCE('Home Location Address: ' + NULLIF(ahc.street, '') + CHAR(10), '') +
			COALESCE('Home Location State: ' + NULLIF(ahc.state, '') + CHAR(10), '') +
			COALESCE('Home Location ZIP / Postal: ' + NULLIF(ahc.zipCode, '') + CHAR(10), '') +
			COALESCE('Home Location Country: ' + NULLIF(ahc.country, '') + CHAR(10), '') +
			COALESCE('Mobile Phone: ' + NULLIF(tp.MobilePhone, '') + CHAR(10), '') +
			COALESCE('Fax: ' + NULLIF(tp.Fax, '') + CHAR(10), '') +
			COALESCE('Home Phone: ' + NULLIF(tp.HomePhone, '') + CHAR(10), '') +
			COALESCE('NOTE: ' + NULLIF(pfc._Red_flags_or_important_notes_, '') + CHAR(10), ''),
			1, 0, '') as Notes
		FROM [dbo].company c
		--LEFT 
		LEFT JOIN Com_Tags t ON c.contactId = t.contactId
		--LEFT JOIN [dbo].[address.home] ah ON c.contactId = ah.contactId
		LEFT JOIN AddHomeCheck ahc ON c.contactId = ahc.contactId
		LEFT JOIN Tmp_Phones tp ON c.contactId = tp.cid
		LEFT JOIN [dbo].[Persons.FirstConversationnotesID206] pfc ON c.contactId = pfc._Person_ID_
		--LEFT JOIN [dbo].[phone.mobile] pm ON c.contactId = pm.contactId
		--LEFT JOIN [dbo].[phone.fax] pf ON c.contactId = pf.contactId
		--LEFT JOIN [dbo].[phone.home] ph ON c.contactId = ph.contactId
		--WHERE ahc.RN = 1
)
		--WHERE ah.contactId NOT IN (SELECT ahc.contactId FROM AddHomeCheck ahc WHERE ahc.RN > 1))


--SELECT * FROM (
--Select ROW_NUMBER() OVER(partition by [company-externalId] Order by [company-externalId]) As Rno, * FROM (
SELECT --TOP(3)
c.contactId AS [company-externalId],
IIF(c.contactId IN (SELECT [contactId] FROM Com_Dup WHERE cdt.RN > 1), CONCAT(COALESCE(cdt.ComName, ''),' ', COALESCE(cdt.RN, '')), iif(c.firstName = '' OR c.firstName IS NULL, 'No CompanyName', c.firstName)) AS [company-name],
--c.firstName AS [company-name],
COALESCE(awc.street, '') AS [company-locationAddress],
COALESCE(awc.state, '') AS [company-locationState],
COALESCE(awc.zipCode, '') AS [company-locationZipCode],
COALESCE((SELECT TOP(1) [Code] FROM [dbo].[VincereCountryCodeDic] vcc WHERE UPPER(COALESCE(awc.country, '')) LIKE UPPER(vcc.[Name])), '') AS [company-locationCountry],
COALESCE(LEFT(wh.url, 100), '') AS [company-website], --limitted by 100 characters
COALESCE(tp.WorkPhone, '') AS [company-phone],
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(tn.Notes, ''),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') AS [company-note]
--note.text AS [ACTIVITIES COMMENTS__1],
--emailHistory.body AS [ACTIVITIES COMMENTS__2]
FROM
[dbo].[company] c
LEFT JOIN AddWorkCheck awc ON c.contactId = awc.contactId
LEFT JOIN WebHome wh ON c.contactId = wh.contactId
--LEFT JOIN [dbo].[address.work] aw ON c.contactId = aw.contactId
--LEFT JOIN [dbo].[website.home] wh ON c.contactId = wh.contactId
LEFT JOIN Tmp_Phones tp ON c.contactId = tp.cid
LEFT JOIN Tmp_Notes tn ON c.contactId = tn.contactId
LEFT JOIN Com_Dup cdt ON c.contactId = cdt.contactId
--WHERE
--awc.RN = 1
--whc.RN = 1
--) abc
--WHERE abc.[company-note] <> ''
--c.contactId IN (43209, 59984)
--) abc) DEF WHERE DEF.Rno > 1