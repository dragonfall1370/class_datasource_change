select count(*) from company

select count(*) from companyx

select count(*) from companyext

select * from companyext

select * from company --204507

select * from contractor

select * from contractext

select * from personx

select * from company_person --724705

select * from candidate --614743

select * from "assignment"

select * from assignmentcandidate

select * from companyassociate
where idcompany = '0003334e-8c44-44ab-ba0d-970c52728bf4'

select * from companystatus

select *
from company_alias

select idcompany, companyname
from company
where idcompany = '45c4068b-e5f3-44e4-9013-dbd88cdbbd2f'

select *
from "alias"
where idalias = 'e6e6d30c-4d43-4877-957b-479560c747cf'

select *
from "user"
where iduser = '65dc2175-55d0-4527-8b47-718a545b36ca'

select idcompany, count(*)
from company_paddress
group by idcompany
having count(*) > 1

select *
from companyaddresstype

select cp.*
, cat.*
from company_paddress cp
left join companyaddresstype cat on cat.idcompanyaddresstype = cp.idcompanyaddresstype
where cp.idcompany = 'fbf515c9-eaa5-4b1a-a2ef-3a90037e681b'

--Company Branch
select *
from "location"
where idlocation = '17380089-7010-40b2-a7f6-2bc2985a1658' --United Arab Emirates

select *
from company_paddress
where idcompany = '000067bf-fb53-4c64-baa3-d84310483e07'

select *
from country

--Company Location
select l."value" 
, c.abbreviation
from "location" l
left join country c on c.value = l.value

--Company Mailing Address (Postal Invoice)


--Research on/by
select researchedon, researchedby, *
from assignmenttarget

select idcompany, count(*)
from assignmenttarget
group by idcompany
having count(*) > 1

--Off limits
select *
from companyofflimit

select idcompany, count(*)
from companyofflimit
group by idcompany
having count(*) > 1

select idcompany
, offlimitby
, offlimitdatefrom
, offlimitdateto
, offlimitnote
, isactive
from companyofflimit

select a.idcompany
	, string_agg(
		concat_ws(chr(10)
			, coalesce('[Off limits] ' || nullif(case when a.isactive = '1' then 'YES' else 'NO' end, ''), NULL)
			, coalesce('[Offlimit by] ' || nullif(a.offlimitby, ''), NULL)
			, coalesce('[Offlimit date from] ' || nullif(a.offlimitdatefrom, ''), NULL)
			, coalesce('[Offlimit date to] ' || nullif(a.offlimitdateto, ''), NULL)
			, coalesce('[Offlimit note] ' || nullif(a.offlimitnote, ''), NULL)
			, chr(10))
		, chr(13)) as company_research
	from companyofflimit a
	--and a.idcompany = '603ca21b-b343-4869-abc0-4b0a2e2d707b'
	group by a.idcompany
	
select *
from offlimittype