
 --Duplicate Company Name
with
temp as (select *,replace(replace(companyExternalId,'NJFS','NJF1S'),'NJFGTP','NJF2GTP') as externalId
from importCompany)

, dup as (select companyExternalId, companyname, ROW_NUMBER() OVER(PARTITION BY companyname ORDER BY externalId ASC) AS rn
from temp)

---Main Script---
select
 c.companyExternalId  as 'company-externalId'
, OriginalName as '(OriginalName)'
, iif(C.companyExternalId in (select companyExternalId from dup where dup.rn > 1)
	, case 
		when left(c.companyExternalId,4) like 'NJFS' then concat('NJF Search - ',dup.companyname)
		when left(c.companyExternalId,4) like 'NJFC' then concat('NJF Contract - ',dup.companyname)
		else concat('NJF GTP - ',dup.companyname)
		end
	, ltrim(C.companyname)) as 'company-name'
--,rn
, companywebsite as 'company-website'
, companyphone as 'company-phone'
, companyswitchBoard as 'company-switchBoard'
, companyfax as 'company-fax'
, companyowners as 'company-owners'
, companydocument as 'company-document'
, companynote as 'company-note'
from importCompany C left join dup on C.companyExternalId = dup.companyExternalId
--where rn>1 or rn>2
order by c.companyname
