use BiGGGroup;

select
--distinct
[status] from bullhorn1.BH_JobResponse
where [status] like '%New Lead%'

select count(*) from bullhorn1.BH_JobResponse

select count(*) from VCApplications
where [application-positionExternalId] not in (
	select [position-externalId] from VCJobs
)

select count(*) from VCApplications
where [application-positionExternalId] not in (
	select [candidate-externalId] from VCCans-- where isDeleted = 0
)

select 'Companies: ' + (select count(*) from VCComs)
+ '  | Contacts: ' + (
select count(*) from VCCons)
+ '  | Jobs: ' + (
select count(*) from VCJobs)
+ '  | Candidates: ' + (
select count(*) from VCCans)
+ '  | Applications: ' + (
select count(*) from VCApplications)

select count(*) from VCCons
where [contact-companyId] in (493, 2816, 2817)

select * from VCComs
where [company-externalId] in (493, 2816, 2817)

select * from tmp_country
where COUNTRY like '%United%'
order by ABBREVIATION

select * from VCCons

select * from bullhorn1.BH_UserContact
where userID in (223
,1364
,1653)



--select * from #EmailTmp5
--where len(email) > 0

--select distinct email from #EmailTmp5
--where len(email) > 0

select ces.emails, Cl.* from bullhorn1.BH_Client Cl
left join dbo.VCConEmails ces on Cl.userID = ces.userID

select
--distinct
UC.userID a1
, UC.email a2
, UC.name a3
, CA.type a4
, iif(charindex('?', trim(isnull(CA.customText15, ''))) > 0, left(trim(isnull(CA.customText15, '')), charindex('?', trim(isnull(CA.customText15, ''))) - 1), trim(isnull(CA.customText15, ''))) as abc
, CA.*
from bullhorn1.Candidate CA
left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID
where CA.isPrimaryOwner = 1
--and CA.isDeleted = 0 
--and CA.type like '%Contract%'
and len(trim(isnull(CA.customText15, ''))) > 0
order by UC.userID

select
CA.customText15,
charindex('?', trim(isnull(CA.customText15, '')))
from bullhorn1.Candidate CA
where candidateID in
(30397)
--(2063, 18032)

--update bullhorn1.BH_UserContact set email = 'Unassigned@no-email.com' where userID = 2063
--update bullhorn1.BH_UserContact set email = 'Jocelyn.Somner@no-email.com' where userID = 18032

select * from bullhorn1.Candidate
--where candidateID = 16395
where isDeleted = 1

select trim(null) abc

select
c.candidateID
, c.salaryLow
, c.salary
from bullhorn1.Candidate C

select * from bullhorn1.BH_Placement