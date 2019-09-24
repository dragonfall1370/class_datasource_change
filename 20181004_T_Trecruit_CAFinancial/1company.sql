

--DOCUMENT
with
 d (id, name) as (SELECT CompanyID
                 , STUFF((SELECT DISTINCT ',' + Nm from DocFolder WHERE CompanyID <> 0  and ContactID = 0 and JobSpecID = 0 and CompanyID = a.CompanyID --and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') 
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select CompanyID from DocFolder where CompanyID <> 0 and ContactID = 0 and JobSpecID = 0) AS a GROUP BY a.CompanyID)
-- select top 100 * from DocFolder where CompanyID <> 0 and ContactID = 0 and JobSpecID = 0
-- select count(*) from d
-- select top 100 * from d

, dup (ClientID,name,rn) as ( SELECT ClientID,ltrim(rtrim( iif(c.CompanyName in ('.',''), cast(c.ClientID as varchar(max)), c.CompanyName) )) as name ,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim( iif(c.CompanyName in ('.',''), cast(c.ClientID as varchar(max)), c.CompanyName) )) ORDER BY c.ClientID ASC) AS rn FROM companies c ) --where name like 'Azurance'
--select * from dup


select 
  o.email as 'company-owners' --, c.UserID
, c.ClientID as 'company-externalId'
, iif(dup.rn > 1, concat(dup.name,' ',dup.rn),dup.NAME) as 'company-name'
         , ltrim(Stuff(
                            Coalesce(' ' + NULLIF(C.Addr1, ''), '')
                        + Coalesce(', ' + NULLIF(C.Addr2, ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
, c.Tel as 'company-switchboard'
, left(c.WebSite,100) as 'Website'
, c.Industry as 'Industry' --<<<
, c.Location as 'company-locationName'

, Stuff( Coalesce('Input Date: ' + NULLIF(cast(C.InputDate as varchar(max)), '') + char(10), '')
       + Coalesce('Website: ' + NULLIF( iif(len(c.website) > 100, cast(C.Website as varchar(max)), '') , '')+ char(10), '')
       + Coalesce('Holding Company: ' + NULLIF(cast(C.HoldingCompany as varchar(max)), '') + char(10), '')
       + Coalesce('Size: ' + NULLIF(cast(C.Size as varchar(max)), '') + char(10), '')
       + Coalesce('Source: ' + NULLIF(cast(C.Source as varchar(max)), '') + char(10), '')
                , 1, 0, '') as note
, d.name as 'company-document'
-- select top 10 * -- select distinct TempConsultant
from companies c
left join d on d.id = c.ClientID
left join owners o on o.id = c.UserID
left join dup on dup.ClientID = c.ClientID;





-- INDUSTRY
select
         distinct c.Industry as 'company-industry'
       , current_timestamp as insert_timestamp
from companies c
WHERE  c.Industry <> '';


with dup (ClientID,name,rn) as ( SELECT ClientID,ltrim(rtrim( iif(c.CompanyName in ('.',''), cast(c.ClientID as varchar(max)), c.CompanyName) )) as name ,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim( iif(c.CompanyName in ('.',''), cast(c.ClientID as varchar(max)), c.CompanyName) )) ORDER BY c.ClientID ASC) AS rn FROM companies c ) --where name like 'Azurance'
select 
         c.ClientID as 'company-externalId'
       , iif(dup.rn > 1, concat(dup.name,' ',dup.rn),dup.NAME) as 'company-name'
       , c.Industry as 'company-industry'
from companies c
left join dup on dup.ClientID = c.ClientID
WHERE  c.Industry <> '';


-- OWNER
select 
 c.ClientID as 'company-externalId'
, o.email as 'company-owners', c.UserID
, replace(o1.email,'stuartwelch@no_email.io','stuart@ca.co.za') as 'owner2', C.TempConsultant
-- select top 10 * -- select distinct TempConsultant
from companies c
left join owners o on o.id = c.UserID
left join owners o1 on o1.id = c.TempConsultant
where c.TempConsultant <> 0
and c.TempConsultant <> c.UserID