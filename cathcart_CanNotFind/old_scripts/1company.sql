
with dup as (SELECT UniqueID,AccountName,ROW_NUMBER() OVER(PARTITION BY C.AccountName ORDER BY C.UniqueID ASC) AS rn FROM clients C ) --where name like 'Azurance'
--select * from dup

      
select
        c.UniqueID as 'company-externalId',
        am.email as 'company-owners', --c.AccountManager as 'company-owners',
        --u.email as 'company-owners',
        --c.ClientName	as 'company-name',
        iif(c.AccountName in (select AccountName from dup where dup.rn > 1),concat(dup.AccountName,' ',dup.rn), iif(c.AccountName = '' or c.AccountName is null,'No CompanyName',c.AccountName)) as 'company-name',
        --c.ContactNumber	as 'company-phone',
        --c.Fax	as 'company-fax',
        ---c.Website	as 'company-website',
        ---c.Contactaddress as 'company-locationName',
	Stuff( 
	                  Coalesce('Business Type: ' + NULLIF(cast(c.BusinessType001 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('ClientStatus: ' + NULLIF(cast(c.ClientStatus as varchar(max)), '') + char(10), '')
                        --+ Coalesce('NormalStartTime: ' + NULLIF(cast(c.NormalStartTime as varchar(max)), '') + char(10), '')
                        --+ Coalesce('NormalEndTime: ' + NULLIF(c.NormalEndTime, '') + char(10), '')
                        + Coalesce('Creation Date: ' + NULLIF(c.CreationDate, '') + char(10), '')
                        + Coalesce('Amendment Date: ' + NULLIF(c.AmendmentDate, '') + char(10), '')
                        + Coalesce('Creating User: ' + NULLIF(am.fullname, '') + char(10), '') -- c.CreatingUser
                        --+ Coalesce('AmendingUser: ' + NULLIF(c.AmendingUser, '') + char(10), '')
                        --+ Coalesce('AccountCode: ' + NULLIF(c.AccountCode, '') + char(10), '')
                , 1, 0, '') as 'company-note',
        c.DocumentPath,
        Stuff(
                          Coalesce(' ' + NULLIF(cast(c.DocumentsNames001 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(cast(c.DocumentsNames002 as varchar(max)), ''), '')
                        + Coalesce(',' + NULLIF(c.DocumentsNames003, ''), '')
                , 1, 1, '') as 'company-document'
-- select count(*) -- select *
from clients c
left join dup on dup.UniqueID = c.UniqueID
left join AccountManager am on am.[user] = c.AccountManager
--left join users u on u.userid = c.AccountManagerId
--left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join client c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = c.clientID

/*
select    c.DocumentPath
        , c.DocumentsNames001
        , c.DocumentsNames002
        , c.DocumentsNames003
from clients c
where c.DocumentsNames001 <> c.DocumentsNames002 and c.DocumentsNames001 <> c.DocumentsNames003 and c.DocumentsNames002 <> c.DocumentsNames003
*/

--select * from clients where accountname = 'Default Company'
--update clients set uniqueID = 'defaultcompany'  where accountname = 'Default Company'