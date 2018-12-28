create table Temp_ContactJobTitle1
(intCompanyTierContactId int PRIMARY KEY,
jobTitleInfo nvarchar(max)
)
go
----------
with tempJobTitle as (select intCompanyTierContactId, jt.intContactId, jt.intCompanyTierId, jt.intJobTitleId, rjt.vchJobTitleName, jt.vchNote
from lJobTitleCompanyTierContact jt left join lCompanyTierContact tc on jt.intContactId = tc.intContactId and jt.intCompanyTierId = tc.intCompanyTierId
		left join refJobTitle rjt on jt.intJobTitleId = rjt.intJobTitleId)

--select distinct intCompanyTierContactId from tempJobTitle
, tempJobTitle1 as (select *, 
			concat(
	  iif(vchJobTitleName = '' or vchJobTitleName is NULL,'',concat('--Job Title: ',vchJobTitleName,char(10)))
	, iif(vchNote = '' or vchNote is NULL,'',concat('  Note: ',vchNote,char(10)))) as jobTitleInfo
from tempJobTitle)

insert into Temp_ContactJobTitle1 SELECT intCompanyTierContactId, 
     STUFF(
         (SELECT char(10) + jobTitleInfo
          from  tempJobTitle1
          WHERE intCompanyTierContactId =tjt1.intCompanyTierContactId
    order by intCompanyTierContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS jobTitleInfo
FROM tempJobTitle1 as tjt1
GROUP BY tjt1.intCompanyTierContactId