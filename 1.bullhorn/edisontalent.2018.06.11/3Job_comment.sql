

with comments as (
                select a.jobPostingID as 'externalid' --, UC.userID, concat(UC1.firstName,' ',UC1.lastName) as fullname
                        , UC.dateAdded
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,UC.dateAdded,120), '') + char(10), '')
                                + Coalesce('Author: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(UC.comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content'
                -- select top 100 *
                from bullhorn1.BH_UserComment UC --where jobPostingID is not null
                left join bullhorn1.BH_JobPosting a on a.jobPostingID = UC.jobPostingID
                left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
                where a.jobPostingID is not null
UNION
	SELECT ---top 20 
       j.jobPostingID as 'externalid' --,concat(UC1.firstName,' ',UC1.lastName) as fullname
	, a.dateAdded
		 , Stuff(       'TASK: ' + char(10)
                            + Coalesce('Candidate: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as varchar(max)), '') + char(10), '') --a.candidateUserID 
                            --+ Coalesce('Assigned To: ' + NULLIF(cast(a.childTaskOwners as varchar(max)), '') + char(10), '')
                            + Coalesce('Contact: ' + NULLIF(cast(concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as varchar(max)), '') + char(10), '') --a.clientUserID 
                            + Coalesce('Due Date And Time: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
                            + Coalesce('Date End: ' + NULLIF(cast(a.dateEnd as varchar(max)), '') + char(10), '')
                            + Coalesce('Description: ' + NULLIF(cast( [dbo].[fn_ConvertHTMLToText](a.description) as varchar(max)), '') + char(10), '')
                            + Coalesce('Visibility: ' + NULLIF(cast(a.isPrivate as varchar(max)), '') + char(10), '')
                            + Coalesce('Job: ' + NULLIF(cast(j.title as varchar(max)), '') + char(10), '') --jobPostingID
                            + Coalesce('Lead: ' + NULLIF(cast( concat(UC3.FirstName,' ',UC3.LastName,'     ',UC3.email) as varchar(max)), '') + char(10), '') --a.leadUserID
                            + Coalesce('Reminder: ' + NULLIF(cast(a.notificationMinutes as varchar(max)), '') + char(10), '')
                            + Coalesce('Opportunity: ' + NULLIF(cast(a.opportunityJobPostingID as varchar(max)), '') + char(10), '')
                            + Coalesce('Placement: ' + NULLIF(cast(a.placementID as varchar(max)), '') + char(10), '')
                            + Coalesce('Priority: ' + NULLIF(cast(a.priority as varchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Day Bits: ' + NULLIF(cast(a.recurrenceDayBits as varchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Frequency: ' + NULLIF(cast(a.recurrenceFrequency as varchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Max: ' + NULLIF(cast(a.recurrenceMax as varchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Month Bits: ' + NULLIF(cast(a.recurrenceMonthBits as varchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Style: ' + NULLIF(cast(a.recurrenceStyle as varchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Type: ' + NULLIF(cast(a.recurrenceType as varchar(max)), '') + char(10), '')
                            + Coalesce('Subject: ' + NULLIF(cast(a.subject as varchar(max)), '') + char(10), '')
                            + Coalesce('Type: ' + NULLIF(cast(a.type as varchar(max)), '') + char(10), '')
                            + Coalesce('Owner: ' + NULLIF(cast( concat(UC4.FirstName,' ',UC4.LastName,'     ',UC4.email) as varchar(max)), '') + char(10), '') --a.userID
                        , 1, 0, '') as content 
        -- select count(*) --24
        -- select top 100 *        
        from bullhorn1.View_Task a
        left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
        left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
        left join bullhorn1.BH_UserContact UC2 ON a.candidateUserID = UC2.userID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_UserContact UC3 ON a.leadUserID = UC3.userID
        left join bullhorn1.BH_UserContact UC4 ON a.userID = UC4.userID
        where j.jobPostingID is not null 
       )

--select count(*) from comments where content <> '' --12428
--select top 10 * from bullhorn1.BH_Client 
--select count(*) from comments where contact_id is not null
--select * from comments where contact_id is not null and contact_id in (4054,7102) --538216 > 563579

select --top 100
                   externalid
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'job' as 'type'
                  , dateAdded as 'insert_timestamp'
                  , content
from comments where note <> '' 
--and candidateID = 2988 or fullname like '%Philip%'                
                

