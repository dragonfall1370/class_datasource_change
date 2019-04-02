--line manager, job type, start date, end date, salary currency, note
--
with tempContacts as (select c.intContactId, c.intPersonId, intCompanyTierContactId, ctc.intCompanyTierId, ct.intCompanyId, ctc.vchJobTitle, c.datLastContacted
		, ctc.vchNote, ctc.bitActive, ctc.intPreferredTelecomId, ROW_NUMBER() OVER(PARTITION BY ctc.intContactId ORDER BY intCompanyId ASC) AS rn
from dContact c left join lCompanyTierContact ctc on c.intContactId = ctc.intContactId
				left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
where ctc.intContactId is not null)

, tempWorkHistory as (select  wh.*, rc.vchCurrencyName, vchCurrencyDesc, ROW_NUMBER() OVER(PARTITION BY wh.intPersonId ORDER BY intWorkHistoryId ASC) AS rn
from dWorkHistory wh left join refCurrency rc on wh.tintSalaryCurrencyId = rc.tintCurrencyId)

, tempWorkHistory1 as (select intPersonId, 
		concat(iif(intWorkHistoryId is NULL,'',concat('-----Employer ',rn,': ',vchCompanyName,char(10)))
			, iif(vchCompanyTierName = '' or vchCompanyTierName is NULL,'',concat('Company Tier Name: ',vchCompanyTierName,char(10)))
			, iif(vchLineManager = '' or vchLineManager is NULL,'',concat('Line Manager: ',vchLineManager,char(10)))
			, iif(vchJobTitle = '' or vchJobTitle is NULL,'',concat('Job Title: ',vchJobTitle,char(10)))
			, case
				 when tintJobType = 1 then concat('Job Type: Permanent (1)',char(10))
				 when tintJobType = 2 then concat('Job Type: Contract (2)',char(10))
				 when tintJobType = 3 then concat('Job Type: Contract (3)',char(10))
				 else concat('Job Type: Permanent (0)',char(10)) end
			--, iif(tintJobType = '' or tintJobType is NULL,'',concat('Job Type (ID): ',tintJobType,char(10)))
			, iif(datStartDate is NULL,'',concat('Start Date: ',datStartDate,char(10)))
			, iif(datEndDate is NULL,'',concat('End Date: ',datEndDate,char(10)))
			, iif(vchCurrencyDesc = '' or vchCurrencyDesc is NULL,'',concat('Currency: ',vchCurrencyDesc,char(10)))
			, iif(vchNote = '' or vchNote is NULL,'',concat('Notes: ', char(10), vchNote,char(10)))
			) as WorkHistory
	, rn
from tempWorkHistory)

, combinedWorkHistory as (SELECT intPersonId, 
     STUFF(
         (SELECT char(10) + WorkHistory
          from  tempWorkHistory1
          WHERE intPersonId =twh.intPersonId
    order by intPersonId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS WorkHistory
FROM tempWorkHistory1 as twh
GROUP BY twh.intPersonId)
--select count(*) from combinedWorkHistory
--select top 100* from combinedWorkHistory
, temp as (select tc.intCompanyTierContactId, tc.intContactId, cwh.*
from tempContacts tc left join combinedWorkHistory cwh on tc.intPersonId = cwh.intPersonId
where cwh.intPersonId is not null)-- and tc.intCompanyId in (2,455))

--select * from temp-- where WorkHistory is not null top 100*

select intCompanyTierContactId as External_Id, -10 as account_user_Id
		, CURRENT_TIMESTAMP as Insert_TimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type
		, concat('-----MIGRATED FROM WORK HISTORY-----',char(10),WorkHistory) as Content
		from temp --where intCompanyTierContactId = 15111
--select * from tempWorkHistory1