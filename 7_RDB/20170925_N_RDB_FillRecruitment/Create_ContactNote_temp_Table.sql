-- create table Temp_Note
--(ObjectId int PRIMARY KEY,
--NotebookInfo nvarchar(max)
--)
--go

--with TempNote as (select ng.NotebookItemId, nl.ObjectId, ng.NotebookType, ng.[From], ng.Recipients, ng.Subject, ng.CreatedOn
-- from VW_NOTEBOOK_GRID ng 
-- left join NotebookLinks nl on ng.NotebookItemId = nl.NotebookItemId
-- --left join Objects o on nl.ObjectId = o.ObjectID
-- where nl.ObjectId in (select ContactPersonId from ClientContacts))

--, TempNote1 as(select ObjectId, 
--	ltrim(rtrim(concat('NotebookType: ', NotebookType
--	, iif([From] = '' or [From] is NULL,'',concat('From: ',[From],', '))
--	, iif(Recipients = '' or Recipients is NULL,'',concat('Recipients: ',Recipients,', '))
--	, iif(Subject = '' or Subject is NULL,'',concat('Subject: ',Subject,', '))
--	, iif(CreatedOn = '' or CreatedOn is NULL,'',concat('Created on: ',convert(varchar(20),CreatedOn, 120),', '))))) as Info
--	from TempNote)

--insert into Temp_CVInfo SELECT ObjectId, 
--     STUFF(
--         (SELECT char(10) + Info
--          from  TempNote1
--          WHERE ObjectId = tn1.ObjectId
--    order by ObjectId asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          ,1,1, '')  AS NotebookInfo
--FROM TempNote1 as tn1
--GROUP BY tn1.ObjectId

-----------------
 create table Temp_Note
(ObjectId int PRIMARY KEY,
NotebookInfo nvarchar(max)
)
go

with TempNote as (select ng.NotebookItemId, nl.ObjectId, ng.NotebookType, ng.[From], ng.Recipients, ng.Subject, ng.CreatedOn, ng.CreatedUserId, ng.CreatedUsername
 from VW_NOTEBOOK_GRID ng 
 left join NotebookLinks nl on ng.NotebookItemId = nl.NotebookItemId
 --left join Objects o on nl.ObjectId = o.ObjectID
 where nl.ObjectId in (select ContactPersonId from ClientContacts))

, TempNote1 as(select ObjectId, 
	ltrim(rtrim(concat('NotebookType: ', NotebookType, ', '
	, iif([From] = '' or [From] is NULL,'',concat('From: ',[From],', '))
	, iif(Recipients = '' or Recipients is NULL,'',concat('Recipients: ',Recipients,', '))
	, iif(Subject = '' or Subject is NULL,'',concat('Subject: ',Subject,', '))
	, iif(CreatedOn = '' or CreatedOn is NULL,'',concat('Created on: ',convert(varchar(20),CreatedOn, 120)))))) as Info
	from TempNote)
select * from TempNote1
select * from VW_NOTEBOOK_GRID





SELECT ObjectId, 
     STUFF(
         (SELECT char(10) + Info
          from  TempNote1
          WHERE ObjectId = tn1.ObjectId
    order by ObjectId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS NotebookInfo
FROM TempNote1 as tn1
GROUP BY tn1.ObjectId
order by ObjectId
select * from Interviews