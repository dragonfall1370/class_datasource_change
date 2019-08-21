
with comments as (
                select Cl.clientID as 'contact_id' --, UC.userID, concat(UC1.firstName,' ',UC1.lastName) as fullname
                        , UC.dateAdded
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,UC.dateAdded,120), '') + char(10), '')
                                + Coalesce('Author: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(UC.comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content'
                -- select top 100 *
                from bullhorn1.BH_UserComment UC
                left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID
                left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
                left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
                where Cl.isPrimaryOwner = 1
UNION ALL
	SELECT --top 20 
	Cl.clientID as 'contact_id' --,concat(UC1.firstName,' ',UC1.lastName) as fullname
	, a.dateAdded
		 , Stuff(       'APPOINTMENT: ' + char(10)
                         	    + Coalesce('Contact: ' + NULLIF(cast(concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as varchar(max)), '') + char(10), '') --a.clientUserID 
                         	    + Coalesce('Candidate: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as varchar(max)), '') + char(10), '') --a.candidateUserID 
                         	    + Coalesce('Job: ' + NULLIF(cast(j.title as varchar(max)), '') + char(10), '') --jobPostingID
                                + coalesce('Communication Method: ' + NULLIF(convert(varchar(max),a.communicationMethod), '') + char(10), '')
		                  + Coalesce('Date Begin: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
                                + coalesce('Date End: ' + NULLIF(convert(varchar(max),a.dateEnd), '') + char(10), '')
                                + coalesce('Type: ' + NULLIF(convert(varchar(max),a.type), '') + char(10), '') --CA.activePlacements
                                + coalesce('Subject: ' + NULLIF(convert(varchar(max),a.subject), '') + char(10), '')
                                + coalesce('Reminder: ' + NULLIF(convert(varchar(max),a.notificationMinutes), '') + char(10), '')
                                + coalesce('Opportunity: ' + NULLIF(convert(varchar(max),j2.title), '') + char(10), '')
                                + coalesce('Location: ' + NULLIF(convert(varchar(max),a.location), '') + char(10), '')
                                + coalesce('Description: ' + NULLIF( [dbo].[fn_ConvertHTMLToText](a.description), '') + char(10), '')
                                + coalesce('File Name: ' + NULLIF(convert(varchar(max),af.name), '') + char(10), '')
                                + coalesce('Owner: ' + NULLIF(cast( concat(UC3.FirstName,' ',UC3.LastName,'     ',UC3.email) as varchar(max)), '') + char(10), '')
                                + coalesce('Lead: ' + NULLIF(cast( concat(UC4.FirstName,' ',UC4.LastName,'     ',UC4.email) as varchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content' 
        -- select count(*) --2062
        -- select top 100 *
        from bullhorn1.View_Appointment a
        left join bullhorn1.View_AppointmentFile af on af.appointmentID = a.appointmentID
        left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = Cl.userID
        left join bullhorn1.BH_UserContact UC2 ON UC2.userID = a.candidateUserID
        left join bullhorn1.BH_UserContact UC3 ON UC3.userID = a.userID
        left join bullhorn1.BH_UserContact UC4 ON UC4.userID = a.LeaduserID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_JobPosting j2 on j.jobPostingID = a.opportunityJobPostingID
        where Cl.userID is not null
UNION ALL
	SELECT ---top 20 
                     Cl.clientID as 'contact_id' --,concat(UC1.firstName,' ',UC1.lastName) as fullname
                     , a.dateAdded
                             , Stuff(       'TASK: ' + char(10)
              + Coalesce('Candidate: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as varchar(max)), '') + char(10), '') --a.candidateUserID 
              --+ Coalesce('Assigned To: ' + NULLIF(cast(a.childTaskOwners as varchar(max)), '') + char(10), '')
              + Coalesce('Contact: ' + NULLIF(cast(concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as varchar(max)), '') + char(10), '') --a.clientUserID 
              + Coalesce('Due Date And Time: ' + NULLIF(cast(a.dateBegin as varchar(max)), '') + char(10), '')
              + Coalesce('Date End: ' + NULLIF(cast(a.dateEnd as varchar(max)), '') + char(10), '')
              + Coalesce('Description: ' + NULLIF(cast( [dbo].[fn_ConvertHTMLToText](a.description) as varchar(max)), '') + char(10), '')
              + Coalesce('Job: ' + NULLIF(cast(j.title as varchar(max)), '') + char(10), '') --jobPostingID
              + Coalesce('Lead: ' + NULLIF(cast( concat(UC3.FirstName,' ',UC3.LastName,'     ',UC3.email) as varchar(max)), '') + char(10), '') --a.leadUserID
              + Coalesce('Reminder: ' + NULLIF(cast(a.notificationMinutes as varchar(max)), '') + char(10), '')
              + Coalesce('Opportunity: ' + NULLIF(cast(a.opportunityJobPostingID as varchar(max)), '') + char(10), '')
              + Coalesce('Placement: ' + NULLIF(cast(a.placementID as varchar(max)), '') + char(10), '')
              + Coalesce('Priority: ' + NULLIF(cast(a.priority as varchar(max)), '') + char(10), '')
              + Coalesce('Subject: ' + NULLIF(cast(a.subject as varchar(max)), '') + char(10), '')
              + Coalesce('Type: ' + NULLIF(cast(a.type as varchar(max)), '') + char(10), '')
              + Coalesce('Owner: ' + NULLIF(cast( concat(UC4.FirstName,' ',UC4.LastName,'     ',UC4.email) as varchar(max)), '') + char(10), '') --a.userID
                        , 1, 0, '') as 'content' 
        -- select count(*) --24
        -- select top 100 *        
        from bullhorn1.View_Task a
        left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
        left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
        left join bullhorn1.BH_UserContact UC2 ON a.candidateUserID = UC2.userID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_UserContact UC3 ON a.leadUserID = UC3.userID
        left join bullhorn1.BH_UserContact UC4 ON a.userID = UC4.userID
        where Cl.userID is not null 
       )

--select count(*) from comments where content <> '' --12428
--select top 10 * from bullhorn1.BH_Client 
--select count(*) from comments where contact_id is not null
--select * from comments where contact_id is not null and contact_id in (4054,7102) --538216 > 563579

select --top 100
                   contact_id
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'contact' as 'type'
                  , dateAdded as 'insert_timestamp'
                  , content
from comments where 'content' <> '' 
--and candidateID = 2988 or fullname like '%Philip%'
        
        