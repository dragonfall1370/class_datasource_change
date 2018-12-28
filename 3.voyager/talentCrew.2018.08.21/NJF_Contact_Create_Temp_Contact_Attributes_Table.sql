create table Temp_ContactAttribute
(intCompanyTierContactId int PRIMARY KEY,
attInfo nvarchar(max)
)
go
--------------------
with tempAttribute as (select intCompanyTierContactId, actc.intContactId, actc.intCompanyTierId, actc.intAttributeId, ra.vchAttributeName, actc.sintAttributeScoreId, ras.vchAttributeScoreName, ras.vchDescription
from lAttributeCompanyTierContact actc left join lCompanyTierContact tc on actc.intContactId = tc.intContactId and actc.intCompanyTierId = tc.intCompanyTierId
		left join refAttribute ra on actc.intAttributeId = ra.intAttributeId
		left join refAttributeScore ras on actc.sintAttributeScoreId = ras.sintAttributeScoreId)
, tempAttribute1 as (select *, 
			concat(
	  iif(vchAttributeName = '' or vchAttributeName is NULL,'',concat('--Attribute Name: ',vchAttributeName,char(10)))
	, iif(vchAttributeScoreName in ('',0) or vchAttributeScoreName is NULL,'',concat('  Score: ',vchAttributeScoreName, iif(vchDescription = '' or vchDescription is null, '',concat(' (',vchDescription,')')),char(10)))) as attinfo
from tempAttribute)

insert into Temp_ContactAttribute SELECT intCompanyTierContactId, 
     STUFF(
         (SELECT char(10) + attinfo
          from  tempAttribute1
          WHERE intCompanyTierContactId =ta1.intCompanyTierContactId
    order by intCompanyTierContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS attInfo
FROM tempAttribute1 as ta1
GROUP BY ta1.intCompanyTierContactId
--------
--select * from Temp_ContactAttribute

