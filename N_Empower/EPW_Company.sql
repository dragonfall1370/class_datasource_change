with dup as (SELECT CompanyId, ltrim(rtrim(replace(Name, char(10),''))) as Name, ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(replace(Name, char(10),''))) ORDER BY CompanyId ASC) AS rn 
FROM Company)

-------Address
, loc as (
		select *, replace(replace(replace(replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(ltrim(rtrim(Street)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(City)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(County)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(PostCode)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(Country)), ''), '')
			+ Coalesce(' (TelNo. ' + NULLIF(ltrim(rtrim(TelNo)), '') + ')', '')
			, 1, 1, '')),char(10),', '),char(13),''),'  ',''),' ,',',') as 'locationName'
	from Address)

-------Get document from company all documents and company logo 
, tempdocs as (
select EntityId, FileName 
from v_DocumentLibrary_AllFields
where Entity = 'Company'
 union all 
select EntityId, right(CompanyLogo,(CHARINDEX('\',Reverse(CompanyLogo))-1)) as FileName
from CompanyConfigFields
where CompanyLogo is not null)

, compdocs as (SELECT EntityId CompanyId,
     STUFF(
         (SELECT ',' + FileName
          from  tempdocs
          WHERE EntityId = da.EntityId
    order by EntityId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS documentName
FROM tempdocs as da
GROUP BY da.EntityId)

--select * from dup where rn>1
---Main Script---
select
  concat('EPW',c.CompanyId) as 'company-externalId'
, C.Name as '(OriginalName)'
, iif(C.companyId in (select CompanyId from dup where dup.rn > 1)
	, iif(dup.Name like '' or dup.Name is NULL,concat('No Company Name - ',dup.CompanyId),concat(dup.Name,'_',dup.rn))
	, iif(C.Name like '' or C.Name is null,concat('No Company Name - ',C.CompanyId),ltrim(rtrim(replace(c.Name, char(10),''))))) as 'company-name'
, iif(WebPage like '' or WebPage not like '%_.__%','',left(convert(varchar(100),c.WebPage),99)) as 'company-website'
--, left(convert(varchar(100),c.WebPage),99) as 'company-website'
, iif(MainTelNo like '' or MainTelNo is NULL,'',MainTelNo) as 'company-phone'
, c.MainFaxNo as 'company-fax'
, cd.documentName as 'company-document'
, left(Concat(
			'Company External ID: EPW', C.CompanyId,char(10)
			--, iif(c.SocialMedia = '','',concat(char(10),'Social Media: ',c.SocialMedia,char(10)))
			, iif(C.Comments like '' or C.Comments is NULL,'',Concat(char(10),'Comments: ',char(10),C.Comments))),32000)
			as 'company-note'
from Company as c
			left join dup on c.CompanyId = dup.CompanyId
			left join compdocs cd on c.CompanyId = cd.CompanyId
UNION ALL
select 'EPW9999999','','Default Company','','','','','This is Default Company from Data Import'
