--drop table Temp_Candidate_WorkHistory

create table Temp_Candidate_WorkHistory
(intCandidateId int PRIMARY KEY,
WorkHistory nvarchar(max)
)
go

with tempWorkHistory as (
select  c.intCandidateId, wh.*, rc.vchCurrencyName, vchCurrencyDesc, ROW_NUMBER() OVER(PARTITION BY wh.intPersonId ORDER BY intWorkHistoryId ASC) AS rn
from dWorkHistory wh left join refCurrency rc on wh.tintSalaryCurrencyId = rc.tintCurrencyId
					 left join dCandidate c on wh.intPersonId = c.intPersonId
where c.intCandidateId is not null)
--select count(*) from tempWorkHistory
--select count(*) from dWorkHistory

, tempWorkHistory1 as (select intCandidateId,-- intPersonId,
		concat(iif(intWorkHistoryId is NULL,'',concat('Company name ',rn,': ',vchCompanyName,char(10)))
			--, iif(vchCompanyTierName = '' or vchCompanyTierName is NULL,'',concat('Company Tier Name: ',vchCompanyTierName,char(10)))
			, iif(vchLineManager = '' or vchLineManager is NULL,'',concat('Line Manager: ',vchLineManager,char(10)))
			, iif(vchJobTitle = '' or vchJobTitle is NULL,'',concat('Job Title: ',vchJobTitle,char(10)))
			--, iif(tintJobType = '' or tintJobType is NULL,'',concat('Job Type (ID): ',tintJobType,char(10)))
			, iif(datStartDate is NULL,'',concat('Start Date: ',datStartDate,char(10)))
			, iif(datEndDate is NULL,'',concat('End Date: ',datEndDate,char(10)))
			, iif(decSalary is NULL,'',concat('Salary: ',decSalary,char(10)))
			, iif(vchCurrencyDesc = '' or vchCurrencyDesc is NULL,'',concat('Currency: ',vchCurrencyDesc,char(10)))
			--, iif(vchNote = '' or vchNote is NULL,'',concat('Notes: ', char(10), vchNote,char(10)))
			) as WorkHistory
	, rn
from tempWorkHistory)
--select top 1000  * from tempWorkHistory1 where WorkHistory like '%Currency: %'
--select count(*) from tempWorkHistory1

insert into Temp_Candidate_WorkHistory SELECT intCandidateId, 
     STUFF(
         (SELECT char(10) + replace(WorkHistory,char(0x0002),'')
          from  tempWorkHistory1
          WHERE intCandidateId =twh.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS WorkHistory
FROM tempWorkHistory1 as twh
GROUP BY twh.intCandidateId
--select count(*) from combinedWorkHistory
--select top 100* from combinedWorkHistory
--, temp as (select tc.intCompanyTierContactId, tc.intContactId, cwh.*
--from tempContacts tc left join combinedWorkHistory cwh on tc.intPersonId = cwh.intPersonId
--where cwh.intPersonId is not null)

--select * from combinedWorkHistory --where intCandidateId = 126-- where WorkHistory is not null top 100*


--select * from dWorkHistory where decSalary = ''