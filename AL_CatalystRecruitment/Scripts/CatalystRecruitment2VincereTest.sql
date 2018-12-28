
--1
--SELECT COUNT(*) FROM [dbo].[address.home]
--21535
SELECT '' as [address.home]
SELECT TOP(3) * FROM [dbo].[address.home] WHERE [contactId] = 7
--2
--SELECT COUNT(*) FROM [dbo].[address.work]
--505
SELECT '' as [address.work]
SELECT TOP(3) * FROM [dbo].[address.work]
--WHERE [contactId] = 7
--3
--SELECT COUNT(*) FROM [dbo].[company]
--185
SELECT '' as [company]
SELECT TOP(3) * FROM [dbo].[company] --WHERE [contactId] = 7
--4
--SELECT COUNT(*) FROM [dbo].[emailHistory]
--1469746
SELECT '' as [emailHistory]
SELECT TOP(3) * FROM [dbo].[emailHistory] WHERE [contactId] = 7
--5
--SELECT COUNT(*) FROM [dbo].[note]
--12480
SELECT '' as [note]
SELECT TOP(3) * FROM [dbo].[note] WHERE [contactId] = 7
--6
--SELECT COUNT(*) FROM [dbo].[Persons.ExtraInformationID205]
--11326
SELECT '' as [Persons.ExtraInformationID205]
SELECT TOP(3) * FROM [dbo].[Persons.ExtraInformationID205]
--7
--SELECT COUNT(*) FROM [dbo].[Persons.FirstConversationnotesID206]
--13
SELECT '' as [Persons.FirstConversationnotesID206]
SELECT TOP(3) * FROM [dbo].[Persons.FirstConversationnotesID206]
--8
--SELECT COUNT(*) FROM [dbo].[phone.fax]
--372
SELECT '' as [phone.fax]
SELECT TOP(3) * FROM [dbo].[phone.fax] WHERE [contactId] = 7
--9
--SELECT COUNT(*) FROM [dbo].[phone.home]
--11317
SELECT '' as [phone.home]
SELECT TOP(3) * FROM [dbo].[phone.home] WHERE [contactId] = 7
--10
--SELECT COUNT(*) FROM [dbo].[phone.mobile]
--24018
SELECT '' as [phone.mobile]
SELECT TOP(3) * FROM [dbo].[phone.mobile] WHERE [contactId] = 7
--11
--SELECT COUNT(*) FROM [dbo].[phone.work]
--1313
SELECT '' as [phone.work]
SELECT TOP(3) * FROM [dbo].[phone.work] WHERE [contactId] = 7
--12
--SELECT COUNT(*) FROM [dbo].[website.home]
--854
SELECT '' as [website.home]
SELECT TOP(3) * FROM [dbo].[website.home] WHERE [contactId] = 7

SELECT distinct c.contactId, ah.street FROM [dbo].[company] c
LEFT JOIN [dbo].[address.home] ah  ON c.contactId = ah.contactId

SELECT distinct abc.contactId FROM (
SELECT
--COUNT(c.contactId)
DISTINCT c.contactId, wp.WorkPhone, mp.MobilePhone, f.Fax, ph.HomePhone
FROM [dbo].[company] c
LEFT JOIN
(SELECT c.contactId, pw.phoneNumber + IIF(pw.extension IS NOT NULL, 'ext' + pw.extension, '') AS [WorkPhone]
FROM [dbo].[company] c
INNER JOIN [dbo].[phone.work] pw ON c.contactId = pw.contactId) AS wp ON c.contactId = wp.contactId
LEFT JOIN
(SELECT c.contactId, pm.phoneNumber + IIF(pm.extension IS NOT NULL, 'ext' + pm.extension, '') AS [MobilePhone]
FROM [dbo].[company] c
INNER JOIN [dbo].[phone.mobile] pm ON c.contactId = pm.contactId) AS mp ON c.contactId = mp.contactId
LEFT JOIN
(SELECT c.contactId, pf.phoneNumber + IIF(pf.extension IS NOT NULL, 'ext' + pf.extension, '') AS [Fax]
FROM [dbo].[company] c
INNER JOIN [dbo].[phone.fax] pf ON c.contactId = pf.contactId) AS f ON c.contactId = f.contactId
LEFT JOIN
(SELECT c.contactId, ph.phoneNumber + IIF(ph.extension IS NOT NULL, 'ext' + ph.extension, '') AS [HomePhone]
FROM [dbo].[company] c
INNER JOIN [dbo].[phone.home] ph ON c.contactId = ph.contactId) AS ph ON c.contactId = ph.contactId
) abc


DECLARE @Tmp_Phones TABLE (
[cid] INT PRIMARY KEY,
[WorkPhone] NVARCHAR(MAX),
[MobilePhone] NVARCHAR(MAX),
[Fax] NVARCHAR(MAX),
[HomePhone] NVARCHAR(MAX));

DECLARE @Tmp_Phones_Dup TABLE (
[cid] INT,
[WorkPhone] NVARCHAR(MAX),
[MobilePhone] NVARCHAR(MAX),
[Fax] NVARCHAR(MAX),
[HomePhone] NVARCHAR(MAX));

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

--INSERT INTO @Tmp_Phones
SELECT  *
FROM    (SELECT [cid], [WorkPhone], [Fax], [HomePhone],
                ROW_NUMBER() OVER (PARTITION BY [WorkPhone] ORDER BY [cid]) AS RowNumber
         FROM   @Tmp_Phones_Dup
         ) b
WHERE   b.RowNumber = 1
ORDER BY b.[cid]

--SELECT * FROM @Tmp_Phones

SELECT * FROM (
Select ROW_NUMBER() OVER(partition by a.WorkPhone Order by a.cid) As Rno, cid, [WorkPhone] FROM
(
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
) a) abcdd

select * from (
SELECT DISTINCT
c.contactId cid,
pw.phoneNumber + IIF(pw.extension IS NOT NULL, 'ext' + pw.extension, '') AS [WorkPhone],
pm.phoneNumber + IIF(pm.extension IS NOT NULL, 'ext' + pm.extension, '') AS [MobilePhone],
pf.phoneNumber + IIF(pf.extension IS NOT NULL, 'ext' + pf.extension, '') AS [Fax],
ph.phoneNumber + IIF(ph.extension IS NOT NULL, 'ext' + ph.extension, '') AS [HomePhone]
FROM [dbo].[company] c
LEFT JOIN [dbo].[phone.work] pw ON c.contactId = pw.contactId
LEFT JOIN [dbo].[phone.mobile] pm ON c.contactId = pm.contactId
LEFT JOIN [dbo].[phone.fax] pf ON c.contactId = pf.contactId
LEFT JOIN[dbo].[phone.home] ph ON c.contactId = ph.contactId
) abc
where abc.cid = 44533

select * from [dbo].[company] where contactid = 44533

--SELECT * FROM 
--( Select ROW_NUMBER() OVER(partition by col1 Order by sno) As Rno ,sno,col1  FROM @tblDup ) tblsub

SELECT * FROM 
( Select ROW_NUMBER() OVER(partition by [contactId] Order by [contactId]) As Rno ,contactId,[url]  FROM [dbo].[website.home] ) tblsub

where tblsub.Rno > 1

SELECT * FROM 
( Select ROW_NUMBER() OVER(partition by /*[contactId]*/ [firstName] Order by [contactId]) As Rno, c.contactId, c.firstName FROM [dbo].[company] c) tblsub

where tblsub.Rno > 1

select * from [dbo].[website.home] where [contactId] IN (
SELECT [contactId] FROM 
( Select ROW_NUMBER() OVER(partition by [contactId] Order by [contactId]) As Rno ,contactId,[url]  FROM [dbo].[website.home] ) tblsub
where tblsub.Rno > 1)

SELECT * FROM 
( Select ROW_NUMBER() OVER(partition by aw.street, aw.city, aw.state, aw.zipCode, aw.country Order by [contactId]) As Rno, contactId, [street], [city], [state], [zipCode], [country]  FROM [dbo].[address.work] aw) tblsub

where tblsub.Rno > 1

SELECT * FROM 
( Select ROW_NUMBER() OVER(partition by [contactId] Order by [contactId]) As Rno, contactId, [street], [city], [state], [zipCode], [country]  FROM [dbo].[address.work] aw) tblsub

where tblsub.Rno > 1

select * from [dbo].[address.work] where [contactId] IN (
SELECT [contactId] FROM 
( Select ROW_NUMBER() OVER(partition by [contactId] Order by [contactId]) As Rno, contactId, [street], [city], [state], [zipCode], [country]  FROM [dbo].[address.work] aw) tblsub
where tblsub.Rno > 1)

--DECLARE @Com_Tags TABLE (
--contactId INT PRIMARY KEY,
--Tags NVARCHAR(MAX));

--DECLARE @Tmp_Notes TABLE (
--contactId INT PRIMARY KEY,
--Notes NVARCHAR(MAX));

---- generate company tags
--INSERT INTO @Com_Tags
--SELECT 
--c.contactId,
--STUFF((SELECT ', ' + ct.tag
--		FROM [dbo].[company.tag] ct
--		WHERE ct.contactId = c.contactId
--		ORDER BY ct.tag
--		FOR XML PATH('')), 1, 1, '') Tags
--	FROM [company] c
--	GROUP BY c.contactId
--	ORDER BY 1

---- generate Notes
--INSERT INTO @Tmp_Notes
--SELECT
--	c.contactId,
--	STUFF(
--		COALESCE('Tags: ' + NULLIF(t.Tags, '') + CHAR(10), '')
--                , 1, 0, '') as Notes
--    FROM [dbo].company c
--    LEFT JOIN @Com_Tags t ON t.contactId = c.contactId

--SELECT TOP(5)
--c.contactId AS [company-externalId],
----company.firstName AS [company-name],
----[address.work].street AS [company-locationAddress],
----[address.work].state AS [company-locationState],
----[address.work].zipCode AS [company-locationZipCode],
----[address.work].country AS [company-locationCountry],
----[website.home].url AS Website,
----[phone.work].phoneNumber AS [company-phone__1],
----[phone.work].extension AS [company-phone__2],
--tn.Notes
----company.tags AS [company-note__1__TAGS],
----[address.home].street AS [company-note__2__Location Address__1],
----[address.home].state AS [company-note__2__Location Address__2],
----[address.home].zipCode AS [company-note__2__Location Address__3],
----[address.home].country AS [company-note__2__Location Address__4],
----[Persons.FirstConversationnotesID206]._Red_flags_or_important_notes_ AS [company-note__3],
----[phone.mobile].phoneNumber AS [company-note__4__PhoneMobile__1],
----[phone.mobile].extension AS [company-note__4__PhoneMobile__2],
----[phone.fax].phoneNumber AS [company-note__5__PhoneFax__1],
----[phone.fax].extension AS [company-note__5__PhoneFax__2],
----[phone.home].phoneNumber AS [company-note__6__PhoneHome__1],
----[phone.home].extension AS [company-note__6__PhoneHome__2],

----note.text AS [ACTIVITIES COMMENTS__1],
----emailHistory.body AS [ACTIVITIES COMMENTS__2]
--FROM
--[dbo].[company] c
--LEFT JOIN @Tmp_Notes tn ON c.contactId = tn.contactId

--SELECT * FROM [dbo].[company]
SELECT TOP(10) * FROM [dbo].[person] p
LEFT JOIN [dbo].[person.tag] pt ON p.contactId = pt.contactId
--WHERE pt.tag

SELECT * FROM [dbo].[person] p WHERE 'client' IN (
	SELECT value
	FROM STRING_SPLIT (
		(SELECT TOP(1) p.tags
		FROM [dbo].[person] p
		WHERE p.contactId = 8
		), '|'
	)
)

select count(*) from company c
left join [address.work] aw ON c.contactId = aw.contactId

select * from company

select count(*) from [dbo].[address.work]

select * from [dbo].[address.work]
where
--co1ntactId is null
contactId = 7

select c.contactId, c.firstName, aw.* from company c
left join [address.work] aw ON c.contactId = aw.contactId
where c.contactId = aw.contactId

select c.contactId, c.firstName, aw.* from company c
left join [address.home] aw ON c.contactId = aw.contactId
where c.contactId = aw.contactId

/****** Script for SelectTopNRows command from SSMS  ******/
--SELECT TOP (1000) [contactId]
--      ,[contactType]
--      ,[firstName]
--      ,[lastName]
--      ,[creationTime]
--      ,[appUserIdCreator]
--      ,[jobTitle]
--      ,[company]
--      ,[source]
--      ,[tags]
--  FROM [CatalystRecruitment].[dbo].[person]

SELECT * FROM [dbo].[company]

SELECT pa.*
FROM [dbo].[person] p
JOIN [dbo].[person.attachment] pa ON p.contactId = pa.contactId

  SELECT p.*, c.contactId
  FROM [dbo].[company] c
  JOIN [dbo].[person] p ON c.firstName = p.company
  GROUP BY c.contactId, p.[contactId]
      ,p.[contactType]
      ,p.[firstName]
      ,p.[lastName]
      ,p.[creationTime]
      ,p.[appUserIdCreator]
      ,p.[jobTitle]
      ,p.[company]
      ,p.[source]
      ,p.[tags]

--andyhopkins|dropbox|tier3|rcvdyc2|bounced|ahnewsletter001|newsletternomore
--andyhopkins|hoppoliupload001|hoppoliupload002|hoppoliupload003|hoppoliupload004|hoppoliupload005|hoppoliupload006|hoppoliupload007|hoppoliupload008|hoppoliupload009|hoppoliupload010|hoppoliupload11|hoppoliupload012|hoppoliupload013|hoppoliupload014|hoppoliupload15

SELECT COUNT(value) FROM STRING_SPLIT((SELECT TOP(1) p.tags FROM [dbo].[person] p WHERE p.contactId = 8), '|') WHERE UPPER(value) = UPPER('client')


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

SELECT
pt.contactId
FROM Person_Tags pt
WHERE (SELECT COUNT(value) FROM STRING_SPLIT(pt.Tags, ',') WHERE UPPER(value) = UPPER('client')) = 0

-- client present 1160 rows
-- client not present 41918 rows

SELECT COUNT(*) FROM [dbo].[person]
-- 43078

SELECT
	p.[contactId]
	,aw.[street]
	,aw.[city]
	,aw.[state]
	,aw.[zipCode]
	,aw.[country]
	FROM
	[dbo].[person] p
	LEFT JOIN [dbo].[address.work] aw ON p.contactId = aw.contactId
	WHERE p.contactId = aw.contactId

SELECT
	c.[contactId] AS cContactId
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
	[dbo].[company] c
	LEFT JOIN [dbo].[address.work] aw ON c.contactId = aw.contactId
	LEFT JOIN [dbo].[address.home] ah ON c.contactId = ah.contactId
	--WHERE p.contactId = aw.contactId OR p.contactId = ah.contactId
	WHERE
	aw.contactId IS NOT NULL
	OR
	ah.contactId IS NOT NULL

SELECT
	p.[contactId] AS pContactId
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
	--WHERE p.contactId = aw.contactId OR p.contactId = ah.contactId
	--WHERE aw.contactId IS NULL AND ah.contactId IS NULL