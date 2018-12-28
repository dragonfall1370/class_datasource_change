USE CatalystRecruitment;

DECLARE @Tmp_Phones TABLE (
[cid] INT PRIMARY KEY,
[WorkPhone] NVARCHAR(MAX),
[MobilePhone] NVARCHAR(MAX),
[Fax] NVARCHAR(MAX),
[HomePhone] NVARCHAR(MAX));

DECLARE @Com_Tags TABLE (
contactId INT PRIMARY KEY,
Tags NVARCHAR(MAX));

DECLARE @Tmp_Notes TABLE (
contactId INT PRIMARY KEY,
Notes NVARCHAR(MAX));

DECLARE @Com_Dup TABLE (
contactId INT,
ComName NVARCHAR(MAX),
RN INT);

-- company with multi address.work check
DECLARE @AddWorkCheck TABLE(
ID INT IDENTITY(1,1),
contactID INT,
RN INT
);

DECLARE @WebHomeCheck TABLE(
ID INT IDENTITY(1,1),
contactID INT,
RN INT
);

DECLARE @AddHomeCheck TABLE(
ID INT IDENTITY(1,1),
contactID INT,
RN INT
);

DECLARE @Tmp_Phones_Dup TABLE(
cid INT,
WorkPhone NVARCHAR(MAX),
MobilePhone NVARCHAR(MAX),
Fax NVARCHAR(MAX),
HomePhone NVARCHAR(MAX)
);

-- create company duplication check
INSERT INTO @Com_Dup
SELECT c.contactId, c.firstName, ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(c.firstName)) ORDER BY c.contactId ASC) AS RN
FROM [dbo].[company] c

--select * from @Com_Dup

INSERT INTO @AddWorkCheck
SELECT o.contactId, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN FROM [dbo].[address.work] o

INSERT INTO @WebHomeCheck
SELECT o.contactId, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN FROM [dbo].[website.home] o

INSERT INTO @AddHomeCheck
SELECT o.contactId, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN FROM [dbo].[address.home] o

INSERT INTO @Tmp_Phones_Dup
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
	ORDER BY [cid]

--WITH
--AddWorkCheck(contactId, RN) AS (SELECT o.contactId, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN FROM [dbo].[address.work] o)
---- company with multi website.home check
--, WebHomeCheck AS (SELECT o.contactId, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN FROM [dbo].[website.home] o)
---- company with multi address.home check
--, AddHomeCheck AS (SELECT o.contactId, ROW_NUMBER() OVER(PARTITION BY o.contactId ORDER BY o.contactId ASC) AS RN FROM [dbo].[address.home] o)
---- create temp phones table
--, Tmp_Phones_Dup AS (
--	SELECT DISTINCT
--	c.contactId [cid],
--	pw.phoneNumber + IIF(pw.extension IS NOT NULL, 'ext' + pw.extension, '') AS [WorkPhone],
--	pm.phoneNumber + IIF(pm.extension IS NOT NULL, 'ext' + pm.extension, '') AS [MobilePhone],
--	pf.phoneNumber + IIF(pf.extension IS NOT NULL, 'ext' + pf.extension, '') AS [Fax],
--	ph.phoneNumber + IIF(ph.extension IS NOT NULL, 'ext' + ph.extension, '') AS [HomePhone]
--	FROM [dbo].[company] c
--	LEFT JOIN [dbo].[phone.work] pw ON c.contactId = pw.contactId
--	LEFT JOIN [dbo].[phone.mobile] pm ON c.contactId = pm.contactId
--	LEFT JOIN [dbo].[phone.fax] pf ON c.contactId = pf.contactId
--	LEFT JOIN[dbo].[phone.home] ph ON c.contactId = ph.contactId
--	--ORDER BY [cid]
--	)







-- generate phones table
INSERT INTO @Tmp_Phones
SELECT 
tpd.cid,
STUFF((SELECT ',' + tpd1.WorkPhone
		FROM @Tmp_Phones_Dup tpd1
		WHERE tpd1.cid = tpd.cid
		ORDER BY tpd1.WorkPhone
		FOR XML PATH('')), 1, 1, ''),
STUFF((SELECT ',' + tpd2.MobilePhone
		FROM @Tmp_Phones_Dup tpd2
		WHERE tpd2.cid = tpd.cid
		ORDER BY tpd2.MobilePhone
		FOR XML PATH('')), 1, 1, ''),
STUFF((SELECT ',' + tpd3.Fax
		FROM @Tmp_Phones_Dup tpd3
		WHERE tpd3.cid = tpd.cid
		ORDER BY tpd3.Fax
		FOR XML PATH('')), 1, 1, ''),
STUFF((SELECT ',' + tpd4.HomePhone
		FROM @Tmp_Phones_Dup tpd4
		WHERE tpd4.cid = tpd.cid
		ORDER BY tpd4.HomePhone
		FOR XML PATH('')), 1, 1, '')
	FROM @Tmp_Phones_Dup tpd
	GROUP BY tpd.cid
	ORDER BY 1

--select * from @Tmp_Phones

-- generate company tags
INSERT INTO @Com_Tags
SELECT 
c.contactId,
STUFF((SELECT ',' + ct.tag
		FROM [dbo].[company.tag] ct
		WHERE ct.contactId = c.contactId
		ORDER BY ct.tag
		FOR XML PATH('')), 1, 1, '') Tags
	FROM [company] c
	GROUP BY c.contactId
	ORDER BY 1

--select distinct contactId from @Com_Tags

-- generate Notes
INSERT INTO @Tmp_Notes
SELECT
	c.contactId,
	STUFF(
		COALESCE('Tags: ' + NULLIF(t.Tags, '') + CHAR(10), '') +
		COALESCE('Home Location Address: ' + NULLIF(ah.street, '') + CHAR(10), '') +
		COALESCE('Home Location State: ' + NULLIF(ah.state, '') + CHAR(10), '') +
		COALESCE('Home Location ZIP / Postal: ' + NULLIF(ah.zipCode, '') + CHAR(10), '') +
		COALESCE('Home Location Country: ' + NULLIF(ah.country, '') + CHAR(10), '') +
		COALESCE('Mobile Phone: ' + NULLIF(tp.MobilePhone, '') + CHAR(10), '') +
		COALESCE('Fax: ' + NULLIF(tp.Fax, '') + CHAR(10), '') +
		COALESCE('Home Phone: ' + NULLIF(tp.HomePhone, '') + CHAR(10), ''),
        1, 0, '') as Notes
    FROM [dbo].company c
	--LEFT 
    LEFT JOIN @Com_Tags t ON c.contactId = t.contactId
	LEFT JOIN [dbo].[address.home] ah ON c.contactId = ah.contactId
	LEFT JOIN @Tmp_Phones tp ON c.contactId = tp.cid
	--LEFT JOIN [dbo].[phone.mobile] pm ON c.contactId = pm.contactId
	--LEFT JOIN [dbo].[phone.fax] pf ON c.contactId = pf.contactId
	--LEFT JOIN [dbo].[phone.home] ph ON c.contactId = ph.contactId
	WHERE ah.contactId NOT IN (SELECT ahc.contactId FROM @AddHomeCheck ahc WHERE ahc.RN > 1)

SELECT * FROM @Tmp_Notes

SELECT --TOP(3)
c.contactId AS [company-externalId],
--IIF(c.contactId IN (SELECT [contactId] FROM @Com_Dup cd WHERE cd.RN > 1), CONCAT(COALESCE(cd.ComName, ''),' ', COALESCE(cd.RN, '')), iif(c.firstName = '' OR c.firstName IS NULL, 'No CompanyName', c.firstName)) AS [company-name],
c.firstName AS [company-name],
aw.street AS [company-locationAddress],
aw.state AS [company-locationState],
aw.zipCode AS [company-locationZipCode],
aw.country AS [company-locationCountry],
LEFT(wh.url, 100) AS [company-website], --limitted by 100 characters
tp.WorkPhone AS [company-phone],
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tn.Notes,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') AS [company-note]

--[Persons.FirstConversationnotesID206]._Red_flags_or_important_notes_ AS [company-note__3],
--note.text AS [ACTIVITIES COMMENTS__1],
--emailHistory.body AS [ACTIVITIES COMMENTS__2]
FROM
[dbo].[company] c
LEFT JOIN [dbo].[address.work] aw ON c.contactId = aw.contactId
LEFT JOIN [dbo].[website.home] wh ON c.contactId = wh.contactId
LEFT JOIN @Tmp_Phones tp ON c.contactId = tp.cid
LEFT JOIN @Tmp_Notes tn ON c.contactId = tn.contactId
WHERE
aw.contactId NOT IN (SELECT awc.contactId FROM @AddWorkCheck awc WHERE awc.RN > 1)
AND aw.contactId NOT IN (SELECT whc.contactId FROM @WebHomeCheck whc WHERE whc.RN > 1)