----------------Get all events of a job to a comment
with tempEvents as (select intJobId, ej.intEventId, sdtEventDate, vchShortname, vchEventDetail, vchEventActionName
from lEventJob ej left join dEvent e on ej.intEventId = e.intEventId
				  left join dUser u on e.intLoggedById = u.intUserId
				  left join svw_EventAction sea on e.sintEventActionId = sea.sintEventActionId)

, JobEvents as (select intJobId,
STUFF(
         (SELECT char(10) + char(10) + 'Event Date: ' + convert(varchar(20),sdtEventDate,120) + char(10) + 'Logged By: ' + vchShortname + char(10)
		  + coalesce('Action: ' + vchEventActionName + char(10), '')
		  + iif(vchEventDetail = '' or vchEventDetail is null,'',concat('Event Detail: ',char(10),vchEventDetail))
          from  tempEvents
          WHERE intJobId = te.intJobId
		  order by sdtEventDate desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 2, '')  AS eventComment
FROM tempEvents as te
GROUP BY te.intJobId)

select intJobId as External_Id, -10 as user_account_Id
		, CURRENT_TIMESTAMP as Insert_TimeStamp, -10 as AssignedUserId, 'comment' as category, 'job' as type
		, eventComment as Content
		from JobEvents --where intCompanyTierContactId = 15111
--select * from tempWorkHistory1