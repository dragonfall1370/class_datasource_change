--
insert into tmp_notebook
select notebookItemId
, case when FileExtension like '%rtf' then dbo.RTF2TXT(Memo)
	when FileExtension like '%html' then dbo.udf_StripHTML(Memo)
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where notebookItemId between 1 and 100000

--
insert into tmp_notebook
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where notebookItemId between 700000 and 760832

--ENHANCED SCRIPTS
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
into tmp_notebook2
from NotebookItemContent
where createdon >= dateadd(year, -1, '2019-08-16 09:08:52.980') --44857 rows

insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between'2017-08-16 09:08:52.980' and '2018-08-16 09:08:52.980' --53313 rows

insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between'2016-08-16 09:08:52.980' and '2017-08-16 09:08:52.980' --63557 rows

insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between'2015-08-16 09:08:52.980' and '2016-08-16 09:08:52.980' --63773 rows

insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between'2014-08-16 09:08:52.980' and '2015-08-16 09:08:52.980' --60837 rows


insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	else Memo end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between'2013-08-16 09:08:52.980' and '2014-08-16 09:08:52.980' --64302 rows


insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	when FileExtension like '%txt' then trim(Memo)
	else NULL end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between '2012-08-16 09:08:52.980' and '2013-08-16 09:08:52.980' --31179 rows


insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2TXT](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	when FileExtension like '%txt' then trim(Memo)
	else NULL end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between '2010-08-16 09:08:52.980' and '2012-08-16 09:08:52.980' --36056 rows
--and NotebookItemId <> 342114 --function not working


insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2Text](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	when FileExtension like '%txt' then trim(Memo)
	else NULL end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between '2008-08-16 09:08:52.980' and '2010-08-16 09:08:52.980' --79780 rows


insert into tmp_notebook2
select notebookItemId
, case when FileExtension like '%rtf' then trim([dbo].[RTF2TXT](Memo))
	when FileExtension like '%html' then trim(dbo.udf_StripHTML([dbo].[udf_StripCSS](Memo)))
	when FileExtension like '%txt' then trim(Memo)
	else NULL end as Memo
, FileExtension
, CreatedUserId
, CreatedOn
from NotebookItemContent
where createdon between '2003-08-16 09:08:52.980' and '2008-08-16 09:08:52.980' --152621 rows


--Audit parsed data
select * from tmp_notebook
where NotebookItemId >= 100000

select count(*) from tmp_notebook

select * from tmp_notebook
where FileExtension like '%html'

select dbo.RTF2TXT(Memo)
, Memo
from NotebookItemContent
where NotebookItemId = 199497