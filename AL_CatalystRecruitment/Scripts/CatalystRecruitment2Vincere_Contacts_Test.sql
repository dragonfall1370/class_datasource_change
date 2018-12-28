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