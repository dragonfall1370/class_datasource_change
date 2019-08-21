
select  vc.vacid as 'position-externalId(origin)'
        ,concat(vc.vacid,"copperq") as 'position-externalId'
	
	, vc.CliPeoID as '(CliPeoID)'
	#, case when (vc.clipeoID = '' OR vc.clipeoID = '0' OR vc.clipeoID is NULL) THEN '23701132' ELSE vc.clipeoID END as 'position-contactId'
	, case when (vc.clipeoID in ('','0') OR vc.clipeoID is NULL) THEN 'defaultcontactcopperq' ELSE vc.clipeoID END as 'position-contactId'
	
        , cl.cliID as '(CompanyID)'
        , cl.cliname as '(company-name)'
    
	, vc.title as 'position-title'
	, vc.novac as 'position-headcount'
	, u.udf1 as 'position-owners' 
	
	, case when ctp.id=1 then 'TEMPORARY_TO_PERMANENT'
               when ctp.id=2 then 'PERMANENT'
               when ctp.id=3 then 'TEMPORARY'
	  end as 'position-type' -- This field only accepts: PERMANENT,INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT
	
	#, as 'position-employmentType'	-- This field only accepts: FULL_TIME, PART_TIME, CASUAL
	#, as 'position-actualSalary'
	, vc.payrate as 'position-payrate'
	, ct.symbol as 'position-currency'
	, vc.description as 'position-publicDescription'
	#, 'position-internalDescription'
	, left(vc.startDate,10) as 'position-startDate'
	, vc.enddate as 'position-endDate'
	, concat(
		case when (vc.clipeoID = '' OR vc.clipeoID = '0' OR vc.clipeoID is NULL) THEN concat('Company Name: ',cl.cliname,char(13)) END
		, case when (vc.chargerate = '' OR vc.chargerate = '0' OR vc.chargerate is NULL) THEN concat('Charge Rate: ',vc.chargerate,char(13)) END
		, case when (vs.status = '' OR vs.status = '0' OR vs.status is NULL) THEN '' else concat('Status: ',vs.status,char(13)) END
		,vc.notes, char(10)
        ) as 'position-Note'
	#, vs.status
# select * #select count(*)
from copperq.vacancy vc
#left join copperq.clipeople cp on vc.clipeoid = cp.CliID
#left join copperq.clipeople cp on vc.CliID = cp.CliID
left join copperq.client cl on vc.cliID = cl.cliID # check companyname
left join copperq.users u ON vc.userid = u.userID
left join copperq.vacstatus vs ON vc.status = vs.statID
left join copperq.currencytbl ct ON vc.currencyid = ct.ID
left join copperq.cantempperm ctp ON vc.tempperm = ctp.ID
#where cl.cliID is not null
#where vc.CliPeoID is null
#where vc.title like '%SAP FI%'

#where 1=1
#and a.status not like '%Archive%'
#and a.clientUserID in (8884, 9273)
#and b.isPrimaryOwner = 1
#where a.jobPostingID = 1096
#where b.userid = 9
#order by a.jobPostingID

# select * from bullhorn1.BH_JobPosting where jobPostingID = 10
# select  from bullhorn1.BH_JobPosting
# select * from bullhorn1.BH_Usercontact where userID = 9

#select UC.name, * from bullhorn1.BH_Client CL left join bullhorn1.BH_Usercontact UC on CL.userID = UC.userID where CL.userID = 19295

#select userid, count(*) from bullhorn1.BH_Client
#where isPrimaryOwner = 1
#group by userID having count(*) > 1

select concat(vc.vacid,"copperq") as 'position-externalId', VacDate  from copperq.vacancy vc where cast(VacDate as date) < '2017-01-01 00:00:00'
