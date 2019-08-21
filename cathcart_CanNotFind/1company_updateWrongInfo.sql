
with dup as ( SELECT UniqueID,AccountName,ROW_NUMBER() OVER(PARTITION BY cast(C.AccountName as varchar(max)) ORDER BY cast(C.UniqueID as varchar(max)) ASC) AS rn FROM clients C ) --where name like 'Azurance'
--select * from dup


select --top 100
        --c.MainSiteUnique, s.SiteUniqueID,
        c.UniqueID as 'externalId',
        iif(c.AccountName in (select AccountName from dup where dup.rn > 1),concat(dup.AccountName,' ',dup.rn), iif(c.AccountName = '' or c.AccountName is null,'No CompanyName',c.AccountName)) as 'company-name',

        -- CORRECT THESE INFOMATION
        null as id,
        --s.organisation as 'company-name',
        s.SitePhoneNumber as 'switchboard',
        replace(replace(replace(s.SiteAddress,' ','<>'),'><',''),'<>',' ')as 'company-locationAddress', -- or REPLACE(REPLACE(REPLACE(s.SiteAddress, '  ', ' ' + CHAR(1)), CHAR(1) + ' ', ''), CHAR(1), '') as address,
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
                , 1, 0, '') as 'company-note'
-- select count(*) --2117 -- select distinct c.BusinessType001 -- select *
from clients c --where c.AccountName like '%ZANROO%'
left join Sites s on s.SiteUniqueID = c.MainSiteUnique
--left join Sites s on s.AccountCode = c.AccountCode 
left join dup on dup.UniqueID = c.UniqueID
left join AccountManager am on am.[user] = c.AccountManager
left join (select ContactUniqueID, concat(Forename,' ',Surname) as mc from contacts) mc on mc.ContactUniqueID = s.MainContactUnique
where s.SiteUniqueID = c.UniqueID
--where c.AccountName like '%ZANROO%'
--where c.MainSiteUnique = '216' or s.SiteUniqueID = '1857'
--where s.SiteUniqueID is null

/*
select c.AccountCode, s.AccountCode, c.MainSiteUnique, s.SiteUniqueID
from clients c
left join Sites s on s.SiteUniqueID = c.MainSiteUnique  where c.AccountCode <> s.AccountCode
*/