
--select C.candidateID, C.FirstName,C.LastName from bullhorn1.Candidate C where FirstName = 'Mohammad' and lastname = 'Abusalah' --C.isPrimaryOwner = 1 and C.candidateID = 33


with comments as (
        SELECT --top 100
                  C.candidateID, C.fullname --,UC.Userid
                , UC.dateAdded
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,UC.dateAdded AT TIME ZONE 'China Standard Time',120), '')   + char(10), '')
                                + Coalesce('Author: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF( 
                                          replace(replace(replace(replace(replace(replace( UC.comments  --[dbo].[fn_ConvertHTMLToText](
                                          ,'Â','')
                                          ,'Â·','')
                                          ,'v\:* {behavior:url(#default#VML);}','')
                                          ,'o\:* {behavior:url(#default#VML);}','')
                                          ,'w\:* {behavior:url(#default#VML);}','')
                                          ,'.shape {behavior:url(#default#VML);}','') , '') + char(10), '')
                        , 1, 0, '') as 'content'
        -- select count(*) --12292
        -- select top 100 *
        from bullhorn1.BH_UserComment UC
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = UC.Userid 
        left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
        where C.isPrimaryOwner = 1 and C.candidateID is not null --and cast(UC.comments as nvarchar(max)) <> ''
UNION ALL
        SELECT --top 100
                  ch.candidateID ,C.fullname --,UC.Userid
                , ch.dateAdded
                , coalesce('HISTORY: ' + char(10)
		                  + 'Date Added: ' + convert(varchar, ch.dateAdded AT TIME ZONE 'China Standard Time', 120) + char(10) 
		                  + 'Candidate History: ' + NULLIF(convert(nvarchar(max),ch.comments), ''), '') 
		                  as 'content'
        -- select count(*) --138
        -- select top 100 *
        from bullhorn1.BH_CandidateHistory CH
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.candidateID = CH.candidateID 
        where C.isPrimaryOwner = 1 and C.candidateID is not null and ch.comments is not null --and cast(ch.comments as varchar(max)) <> '' 
UNION ALL
	SELECT --top 100
	C.candidateID ,C.fullname  --, a.candidateuserID, C.userID
	, a.dateAdded
		 , Stuff(       'APPOINTMENT: ' + char(10)
                         	    + Coalesce('Contact: ' + NULLIF(cast(concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as nvarchar(max)), '') + char(10), '') --a.clientUserID 
                         	    + Coalesce('Candidate: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as nvarchar(max)), '') + char(10), '') --a.candidateUserID 
                         	    + Coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), '') + char(10), '') --jobPostingID
                                + coalesce('Communication Method: ' + NULLIF(convert(nvarchar(max),a.communicationMethod), '') + char(10), '')
		                  + Coalesce('Date Begin: ' + NULLIF(cast(a.dateBegin AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                                + coalesce('Date End: ' + NULLIF(convert(nvarchar(max),a.dateEnd AT TIME ZONE 'China Standard Time'), '') + char(10), '')
                                + coalesce('Type: ' + NULLIF(convert(nvarchar(max),a.type), '') + char(10), '') --CA.activePlacements
                                + coalesce('Subject: ' + NULLIF(convert(nvarchar(max),a.subject), '') + char(10), '')
                                + coalesce('Reminder: ' + NULLIF(convert(nvarchar(max),a.notificationMinutes), '') + char(10), '')
                                + coalesce('Opportunity: ' + NULLIF(convert(nvarchar(max),j2.title), '') + char(10), '')
                                + coalesce('Location: ' + NULLIF(convert(nvarchar(max),a.location), '') + char(10), '')
                                + coalesce('Description: ' + NULLIF( a.description, '') + char(10), '') --[dbo].[fn_ConvertHTMLToText](
                                + coalesce('File Name: ' + NULLIF(convert(nvarchar(max),af.name), '') + char(10), '')
                                + coalesce('Owner: ' + NULLIF(cast( concat(UC3.FirstName,' ',UC3.LastName,'     ',UC3.email) as nvarchar(max)), '') + char(10), '')
                                + coalesce('Lead: ' + NULLIF(cast( concat(UC4.FirstName,' ',UC4.LastName,'     ',UC4.email) as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'content' 
        -- select count(*) --2062
        -- select top 20 *
        from bullhorn1.View_Appointment a
        left join bullhorn1.View_AppointmentFile af on af.appointmentID = a.appointmentID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = a.candidateuserID
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = a.ClientuserID
        left join bullhorn1.BH_UserContact UC2 ON UC2.userID = a.candidateUserID
        left join bullhorn1.BH_UserContact UC3 ON UC3.userID = a.userID
        left join bullhorn1.BH_UserContact UC4 ON UC4.userID = a.LeaduserID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_JobPosting j2 on j.jobPostingID = a.opportunityJobPostingID        
        where C.isPrimaryOwner = 1 and C.candidateID is not null 
UNION ALL
	SELECT --top 100
	C.candidateID,C.fullname  --, a.candidateuserID, C.userID
	, a.dateAdded
		 , Stuff(       'TASK: ' + char(10)
                            + Coalesce('Candidate: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as nvarchar(max)), '') + char(10), '') --a.candidateUserID 
                            --+ Coalesce('Assigned To: ' + NULLIF(cast(a.childTaskOwners as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Contact: ' + NULLIF(cast(concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as nvarchar(max)), '') + char(10), '') --a.clientUserID 
                            + Coalesce('Due Date And Time: ' + NULLIF(cast(a.dateBegin AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Date End: ' + NULLIF(cast(a.dateEnd AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Description: ' + NULLIF(cast( a.description as nvarchar(max)), '') + char(10), '') --[dbo].[fn_ConvertHTMLToText](
                            + Coalesce('Visibility: ' + NULLIF(cast(a.isPrivate as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), '') + char(10), '') --jobPostingID
                            + Coalesce('Lead: ' + NULLIF(cast( concat(UC3.FirstName,' ',UC3.LastName,'     ',UC3.email) as nvarchar(max)), '') + char(10), '') --a.leadUserID
                            + Coalesce('Reminder: ' + NULLIF(cast(a.notificationMinutes as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Opportunity: ' + NULLIF(cast(j2.title as nvarchar(max)), '') + char(10), '') --a.opportunityJobPostingID 
                            + Coalesce('Placement: ' + NULLIF(cast(a.placementID as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Priority: ' + NULLIF(cast(a.priority as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Day Bits: ' + NULLIF(cast(a.recurrenceDayBits as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Frequency: ' + NULLIF(cast(a.recurrenceFrequency as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Max: ' + NULLIF(cast(a.recurrenceMax as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Month Bits: ' + NULLIF(cast(a.recurrenceMonthBits as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Style: ' + NULLIF(cast(a.recurrenceStyle as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Recurrence Type: ' + NULLIF(cast(a.recurrenceType as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Subject: ' + NULLIF(cast(a.subject as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Type: ' + NULLIF(cast(a.type as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Owner: ' + NULLIF(cast( concat(UC4.FirstName,' ',UC4.LastName,'     ',UC4.email) as nvarchar(max)), '') + char(10), '') --a.userID
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
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = a.candidateuserID 
        left join bullhorn1.BH_JobPosting j2 on j2.jobPostingID = a.opportunityJobPostingID
        where C.isPrimaryOwner = 1 and C.candidateID is not null 
UNION ALL
       -- PLACEMENT
        select --top 100
        C.candidateid, C.fullname
        , pl.dateadded 
	, Stuff( 'PLACEMENT: ' + char(10)
              + Coalesce('Billing Contact: ' + NULLIF(cast( concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email)  as nvarchar(max)), '') + char(10), '')  --pl.billingUserID
                     --+ Coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as nvarchar(max)), '') + char(10), '')
              + Coalesce('Bill Rate: ' + NULLIF(cast(pl.clientBillRate as nvarchar(max)), '') + char(10), '')
              + Coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as nvarchar(max)), '') + char(10), '')
              + Coalesce('Comments: ' + NULLIF(cast(pl.comments as nvarchar(max)), '') + char(10), '')
                     --+ Coalesce('Contract Employment Info: ' + NULLIF(cast(pl.contractInfoHeader as nvarchar(max)), '') + char(10), '')
              + Coalesce('Primary Timesheet Approver: ' + NULLIF(cast(pl.correlatedCustomText1 as nvarchar(max)), '') + char(10), '')
              + Coalesce('Secondary Timecard Approver: ' + NULLIF(cast(pl.correlatedCustomText2 as nvarchar(max)), '') + char(10), '')
              + Coalesce('Purchase Order Number: ' + NULLIF(cast(pl.correlatedCustomText3 as nvarchar(max)), '') + char(10), '')
              + Coalesce('Cost Center: ' + NULLIF(cast(pl.costCenter as nvarchar(max)), '') + char(10), '')
              + Coalesce('Insurance Reference: ' + NULLIF(cast(pl.customText1 as nvarchar(max)), '') + char(10), '')
              + Coalesce('Start Date: ' + NULLIF(cast(pl.dateBegin AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
              + Coalesce('Effective Date (Client): ' + NULLIF(cast(pl.dateClientEffective AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
              + Coalesce('Effective Date: ' + NULLIF(cast(pl.dateEffective AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
              + Coalesce('Scheduled End: ' + NULLIF(cast(pl.dateEnd AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
              + Coalesce('Days Guaranteed: ' + NULLIF(cast(pl.daysGuaranteed as nvarchar(max)), '') + char(10), '')
              + Coalesce('Days Pro-Rated: ' + NULLIF(cast(pl.daysProRated as nvarchar(max)), '') + char(10), '')
              + Coalesce('Employment Type: ' + NULLIF(cast(pl.employmentType as nvarchar(max)), '') + char(10), '')
              + Coalesce('Placement Fee (%): ' + NULLIF(cast(pl.fee as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Placement Fee (Flat): ' + NULLIF(cast(pl.flatFee as nvarchar(max)), '') + char(10), '')
              + Coalesce('Hours of Operation: ' + NULLIF(cast(pl.hoursOfOperation as nvarchar(max)), '') + char(10), '')
              + Coalesce('Hours Per Day: ' + NULLIF(cast(pl.hoursPerDay as nvarchar(max)), '') + char(10), '')
              + Coalesce('Rate Entry Type: ' + NULLIF(cast(pl.isMultirate as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Mark-up %: ' + NULLIF(cast(pl.markUpPercentage as nvarchar(max)), '') + char(10), '')
              + Coalesce('Over-time Pay Rate: ' + NULLIF(cast(pl.overtimeRate as nvarchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(pl.payRate as nvarchar(max)), '') + char(10), '')
                     --+ Coalesce('Pay Rate Information: ' + NULLIF(cast(pl.payRateInfoHeader as nvarchar(max)), '') + char(10), '')
                     --+ Coalesce('Permanent Employment Info: ' + NULLIF(cast(pl.permanentInfoHeader as nvarchar(max)), '') + char(10), '')
              + Coalesce('Referral Fee Type: ' + NULLIF(cast(pl.referralFeeType as nvarchar(max)), '') + char(10), '')
              + Coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as nvarchar(max)), '') + char(10), '')
              + Coalesce('Salary: ' + NULLIF(cast(pl.salary as nvarchar(max)), '') + char(10), '')
              + Coalesce('Pay Unit: ' + NULLIF(cast(pl.salaryUnit as nvarchar(max)), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(cast(pl.status as nvarchar(max)), '') + char(10), '')
        , 1, 0, '') as 'content'
        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid
UNION ALL
       -- Job Submission
        select top 100
        C.candidateid, C.fullname
        , jr.dateadded
	, Stuff(    'JOB SUBMISSION: ' + char(10)
                 --Coalesce('Appointments: ' + NULLIF(cast(jr.appointments as nvarchar(max)), '') + char(10), '')
              + Coalesce('Bill Rate: ' + NULLIF(cast(jr.billRate as nvarchar(max)), '') + char(10), '')
              + Coalesce('Comments: ' + NULLIF(cast(jr.comments as nvarchar(max)), '') + char(10), '')
              + Coalesce('Date Added: ' + NULLIF(cast(jr.dateAdded AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Date Last Modified: ' + NULLIF(cast(jr.dateLastModified as nvarchar(max)), '') + char(10), '')
              + Coalesce('Date Web Response: ' + NULLIF(cast(jr.dateWebResponse AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
              + Coalesce('Job Order: ' + NULLIF(cast(j.title as nvarchar(max)), '') + char(10), '') --jr.jobPostingID
              + Coalesce('Latest Appointment: ' + NULLIF(cast(jr.latestAppointmentID as nvarchar(max)), '') + char(10), '')
              + Coalesce('Migrate GUID: ' + NULLIF(cast(jr.migrateGUID as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Owners: ' + NULLIF(cast(jr.owners as nvarchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(jr.payRate as nvarchar(max)), '') + char(10), '')
              + Coalesce('Salary: ' + NULLIF(cast(jr.salary as nvarchar(max)), '') + char(10), '')
              + Coalesce('Added By: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as nvarchar(max)), '') + char(10), '') --jr.sendingUserID
              + Coalesce('Source: ' + NULLIF(cast(jr.source as nvarchar(max)), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(cast(jr.status as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Task: ' + NULLIF(cast(jr.tasks as nvarchar(max)), '') + char(10), '')
              + Coalesce('Candidate: ' + NULLIF(cast( concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as nvarchar(max)), '') + char(10), '') --jr.userID 
        , 1, 0, '') as note
        from bullhorn1.View_JobResponse jr
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = jr.userID
        left join bullhorn1.BH_UserContact UC2 ON UC2.userID = jr.sendingUserID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = jr.jobPostingID        
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = jr.userid
        --where jr.userid <> jr.sendingUserID
)


--select count(*) from comments where note <> '' --12428
select --top 100
                   candidateID
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'candidate' as 'type'
                  , dateAdded as 'insert_timestamp'
                  , [dbo].[fn_ConvertHTMLToText](content) as content
from comments where content <> '' 
--and candidateID = 2988 or fullname like '%Philip%'




