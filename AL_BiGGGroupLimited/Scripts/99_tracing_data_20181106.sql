select count(*) from bullhorn1.BH_UserContact uc
where len(trim(isnull(uc.middleName, ''))) > 0

select count(*) from bullhorn1.BH_UserContact uc
where len(trim(isnull(uc.phone, ''))) > 0
or len(trim(isnull(uc.phone2, ''))) > 0
or len(trim(isnull(uc.phone3, ''))) > 0
or len(trim(isnull(uc.workPhone, ''))) > 0

--11690/51778

select count(*) from bullhorn1.BH_UserContact uc
where uc.firstName = 'Default Contact' and len(trim(isnull(uc.occupation, ''))) > 0

select count(*) from bullhorn1.BH_UserContact uc
where len(trim(isnull(uc.status, ''))) > 0

17844/51778

select count(*) from bullhorn1.BH_ClientCorporation cc
where len(trim(isnull(cc.companyURL, ''))) > 0
--1658/5151
select count(*) from bullhorn1.BH_ClientCorporation cc
where len(trim(isnull(cc.phone, ''))) > 0 or len(trim(isnull(cc.billingPhone, ''))) > 0
--2091/5151

select count(*) from bullhorn1.BH_ClientCorporation cc
where len(trim(isnull(cc.phone, ''))) > 0 or len(trim(isnull(cc.billingPhone, ''))) > 0

select
x.candidateID
, firstName
, middleName
, lastName
, email
, email2
, email3
, email_old
, externalEmail
, [status]
, isDeleted
, getdate() - 1 as deleted_timestamp
from bullhorn1.Candidate x
where x.isDeleted = 1


select
x.jobPostingID as JobExtId
, x.isOpen
, dateClosed
, case x.isOpen
	when 1 then dateadd(month, 6, getdate())
	when 0 then iif(x.dateClosed is null, getdate() - 3, x.dateClosed)
end as head_count_close_date
from bullhorn1.BH_JobPosting x


select count([company-externalID]) IdCount, count(distinct [company-name]) nameCount from VCComs

select distinct [candidate-email] from VCCans

select count(*) from VCCons where [contact-companyId] not in
(
	select [company-externalId] from VCComs
)

select count(*) from VCJobs
where [position-contactId] is null or [position-contactId] not in (
	select [contact-externalId] from VCCons
)

select count(*) from VCApplications
where [application-positionExternalId] not in (
	select [position-externalId] from VCJobs
)

select count(*) from VCApplications
where [application-positionExternalId] not in (
	select [candidate-externalId] from VCCans-- where isDeleted = 0
)

