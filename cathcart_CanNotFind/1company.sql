
with dup as ( SELECT UniqueID,AccountName,ROW_NUMBER() OVER(PARTITION BY cast(C.AccountName as varchar(max)) ORDER BY cast(C.UniqueID as varchar(max)) ASC) AS rn FROM clients C ) --where name like 'Azurance'
--select * from dup

      
select --top 100
        c.UniqueID as 'company-externalId',
        am.email as 'company-owners', --c.AccountManager as 'company-owners',
        --u.email as 'company-owners',
        --c.ClientName	as 'company-name',
        iif(c.AccountName in (select AccountName from dup where dup.rn > 1),concat(dup.AccountName,' ',dup.rn), iif(c.AccountName = '' or c.AccountName is null,'No CompanyName',c.AccountName)) as 'company-name',
        --s.organisation as 'company-name',
        s.SitePhoneNumber as 'switchboard',
        s.SiteAddress as 'company-locationAddress',
        s.SiteAddressLine4 as 'District',
        s.SiteAddressLine5 as 'company-locationCity',
        case s.SiteAddressLine6 
                when 'SCOTLAND' then 'GB'
                when 'SINGAPORE' then 'SG'
                when 'UNITED KINGDOM' then 'GB'
                else 'TH' end as 'company-locationCountry',
        s.SitePostcode as 'company-locationZipCode',
        ltrim(Stuff( 
                  Coalesce(' ' + NULLIF(cast(s.SiteAddressLine4 as varchar(max)), '') + char(10), '')
                + Coalesce(', ' + NULLIF(cast(s.SiteAddressLine5 as varchar(max)), '') + char(10), '')
                + Coalesce(', ' + NULLIF(cast(s.SiteAddressLine6 as varchar(max)), '') + char(10), '')
                , 1, 1, '') ) as 'LocationName',
        s.SiteFaxNumber as 'company-fax',
        s.WebAddress as 'company-website',

        --c.ContactNumber	as 'company-phone',
        --c.Fax	as 'company-fax',
        ---c.Website	as 'company-website',
        ---c.Contactaddress as 'company-locationName',
	Stuff(
	                  Coalesce('Locality: ' + NULLIF(cast(s.Locality as varchar(max)), '') + char(10), '')
	                + Coalesce('Primary Site YN: ' + NULLIF(cast(s.PrimarySiteYN as varchar(max)), '') + char(10), '')
	                + Coalesce('Short Name: ' + NULLIF(cast(s.ShortName as varchar(max)), '') + char(10), '')
	                + Coalesce('Email: ' + NULLIF(cast(s.Email as varchar(max)), '') + char(10), '')
	                + Coalesce('Main Contact Unique: ' + NULLIF(cast(mc.mc as varchar(max)), '') + char(10), '') --s.MainContactUnique
	                + Coalesce('Account Code: ' + NULLIF(cast(s.AccountCode as varchar(max)), '') + char(10), '')
	                + Coalesce('Business Type: ' + NULLIF(cast(c.BusinessType001 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('ClientStatus: ' + NULLIF(cast(c.ClientStatus as varchar(max)), '') + char(10), '')
                        --+ Coalesce('NormalStartTime: ' + NULLIF(cast(c.NormalStartTime as varchar(max)), '') + char(10), '')
                        --+ Coalesce('NormalEndTime: ' + NULLIF(c.NormalEndTime, '') + char(10), '')
                        + Coalesce('Creation Date: ' + NULLIF(cast(c.CreationDate as varchar(max)), '') + char(10), '')
                        + Coalesce('Amendment Date: ' + NULLIF(cast(c.AmendmentDate as varchar(max)), '') + char(10), '')
                        + Coalesce('Creating User: ' + NULLIF(am.fullname, '') + char(10), '') -- c.CreatingUser
                        --+ Coalesce('AmendingUser: ' + NULLIF(c.AmendingUser, '') + char(10), '')
                        --+ Coalesce('AccountCode: ' + NULLIF(c.AccountCode, '') + char(10), '')
                , 1, 0, '') as 'company-note',
        c.DocumentPath,
        Stuff(
                          Coalesce(' ' + NULLIF(cast(c.DocumentsNames001 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames002 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(c.DocumentsNames003, ''), '')
                        --+ Coalesce(',' + NULLIF(replace(s.DocumentDirectory,'F:\cathcart\Influence_Docs/Contacts/',''), ''), '')
                , 1, 1, '') as 'company-document'
-- select count(*) --2117 -- select distinct c.BusinessType001 -- select *
from clients c --where c.AccountName like '%ZANROO%'
left join dup on dup.UniqueID = c.UniqueID
left join AccountManager am on am.[user] = c.AccountManager
--left join Sites s on s.SiteUniqueID = c.UniqueID
left join Sites s on s.SiteUniqueID = c.MainSiteUnique
left join (select ContactUniqueID, concat(Forename,' ',Surname) as mc from contacts) mc on mc.ContactUniqueID = s.MainContactUnique
where c.AccountName like '%MINDTERRA%'
--left join users u on u.userid = c.AccountManagerId
--left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join client c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = c.clientID
--where c.AccountName like '%cathcart%'
--select distinct SiteAddressLine6 from Sites where SiteAddressLine6 not like '%thai%' and SiteAddressLine6 not like '%bak%' and SiteAddressLine6 not like '%bang%'

/*
select    c.DocumentPath
        , c.DocumentsNames001
        , c.DocumentsNames002
        , c.DocumentsNames003
from clients c
where c.DocumentsNames001 <> c.DocumentsNames002 and c.DocumentsNames001 <> c.DocumentsNames003 and c.DocumentsNames002 <> c.DocumentsNames003

--select * from clients where accountname = 'Default Company'
--update clients set uniqueID = 'defaultcompany'  where accountname = 'Default Company'

*/
/*
select count(*) --2117 -- select distinct c.BusinessType001 -- 
-- select c.AccountCode,   c.DocumentPath, c.DocumentsNames001, c.DocumentsNames002, c.DocumentsNames003, s.AccountCode, s.DocumentDirectory
from clients c --where c.AccountName like '%ZANROO%'
left join Sites s on s.SiteUniqueID = c.MainSiteUnique
--left join Sites s on s.AccountName = c.AccountName
where 
c.AccountCode <> s.AccountCode
c.AccountName like '%ZANROO%'

select top 100 ContactUniqueID, SiteUniqueID, concat(Forename,' ',Surname) as mc 
select top 10 * from contacts
*/