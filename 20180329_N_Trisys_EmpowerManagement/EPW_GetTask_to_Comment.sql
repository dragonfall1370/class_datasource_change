select concat('EPW',tc.ContactId) as CanExternalId, -10 as userId
		, t.DateLastUpdated as InsertTimeStamp
		, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		, tc.TaskId
		, concat('--MIGRATED FROM TASK--',
				--iif(event_make_datetime = '' or event_make_datetime = 'null', '', concat(char(10), 'Event Make Date Time: ',event_make_datetime)),
				iif(t.StartDate is null, '', concat(char(10),'Start Date: ',t.StartDate)),
				iif(t.EndDate is null, '', concat(char(10),'End Date: ',t.EndDate)),
				iif(ta.TaskType = '' or ta.TaskType = 'null', '', concat(char(10),'Task Type: ',ta.TaskType)),
				iif(ta.TaskUsers = '' or ta.TaskUsers is null, '', concat(char(10),'Owner: ',ta.TaskUsers)),
				iif(ta.TaskContacts = '' or ta.TaskContacts is null, '', concat(char(10),'Contacts: ',ta.TaskContacts)),
				iif(t.Description = '' or t.Description is null, '', concat(char(10),'Description: ',char(10),t.Description)),
				concat(char(10),'Task ID: ', t.TaskId)
				) as commentContent
from TaskContacts tc --left join TaskType tt on t.TaskTypeId = tt.TaskTypeId
			left join Task t on tc.TaskId = t.TaskId
			left join v_Task_AllFields_WithContactInfo ta on tc.TaskId = ta.TaskId
--where tc.TaskId is not null
order by tc.ContactId