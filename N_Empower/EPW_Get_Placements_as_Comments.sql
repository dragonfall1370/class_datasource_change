select concat('EPW',pr.RequirementId) as jobExternalId, -10 as userId
		, pc.DateLastUpdated as InsertTimeStamp
		, -10 as AssignedUserId, 'comment' as category, 'job' as type
		, pc.EntityId, pc.ReportingTo
		, concat('--MIGRATED FROM PLACEMENT--',
				--iif(event_make_datetime = '' or event_make_datetime = 'null', '', concat(char(10), 'Event Make Date Time: ',event_make_datetime)),
				iif(pa.CandidateContact_FullName = '' or pa.CandidateContact_FullName is null, '', concat(char(10),'Candidate: ',pa.CandidateContact_FullName)),
				iif(pa.PlacementContact_FullName = '' or pa.PlacementContact_FullName is null, '', concat(char(10),'Contact: ',pa.PlacementContact_FullName)),
				iif(pa.Placement_JobTitle = '' or pa.Placement_JobTitle is null, '', concat(char(10),'Job Title: ',pa.Placement_JobTitle)),
				iif(pc.StartDate is null, '', concat(char(10),'Start Date: ',pc.StartDate)),
				iif(pc.Location = '' or pc.Location is null, '', concat(char(10),'Location: ',replace(replace(replace(pc.Location,char(10),''),char(13),', '),' ,',','))),
				iif(pc.Salary = '' or pc.Salary is null, '', concat(char(10),'Salary: ',pc.Salary)),
				iif(pc.Bonus = '' or pc.Bonus is null, '', concat(char(10),'Bonus: ',pc.Bonus)),
				iif(pc.Benefits is null, '', concat(char(10),'Benefits: ',pc.Benefits)),
				iif(pc.FeePercentage is null, '', concat(char(10),'Fee Percentage: ',pc.FeePercentage)),
				iif(pc.Fee = '' or pc.Fee is null, '', concat(char(10),'Fee: ',pc.Fee)),
				iif(pc.InvoiceDate is null, '', concat(char(10),'Invoice Date: ',pc.InvoiceDate)),
				iif(pc.Description = '' or pc.Description is null, '', concat(char(10),'Description: ',char(10),pc.Description)),
				concat(char(10),'Placement ID: ', pc.EntityId)
				) as commentContent
from PlacementConfigFields pc--left join TaskType tt on t.TaskTypeId = tt.TaskTypeId
			left join PlacementRequirement pr on pc.EntityId = pr.PlacementId
			left join PlacementContact pcon on pc.EntityId = pcon.PlacementId
			left join v_Placement_AllFields pa on pc.EntityId = pa.Placement_PlacementId
--where tc.TaskId is not null
order by pc.EntityId


--select * from PlacementContact