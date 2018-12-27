/****** Script for SelectTopNRows command from SSMS  ******/
SELECT a.Body, b.id,
 a.CreatedDate as insert_timestamp,
'comment' as 'category', 
'company' as 'type',
-10 as 'user_account_id'

  FROM [Zitko].[dbo].[Note] a
  left join Account b on a.ParentId = b.Id
  where b.id is not null


