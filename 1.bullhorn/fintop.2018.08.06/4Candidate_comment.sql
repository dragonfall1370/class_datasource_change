
--select C.candidateID, C.FirstName,C.LastName from bullhorn1.Candidate C where FirstName = 'Mohammad' and lastname = 'Abusalah' --C.isPrimaryOwner = 1 and C.candidateID = 33


with comments as (
        SELECT --top 1000
                  C.candidateID, C.fullname --,UC.Userid
                , UC.dateAdded
                        , Stuff('COMMENT: ' + char(10)
                                + Coalesce('Created Date: ' + NULLIF(convert(varchar,UC.dateAdded,120), '') + char(10), '')
                                + Coalesce('Author: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF( 
                                          replace(replace(replace(replace(replace(replace( [dbo].[fn_ConvertHTMLToText](UC.comments)
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
        where C.isPrimaryOwner = 1 and C.candidateID is not null --and cast(UC.comments as varchar(max)) <> ''
UNION ALL
        SELECT --top 1000
                  ch.candidateID ,C.fullname --,UC.Userid
                , ch.dateAdded
                , coalesce('HISTORY: ' + char(10)
		                  + 'Date Added: ' + convert(varchar(10), ch.dateAdded, 120) + char(10) 
		                  + 'Candidate History: ' + NULLIF(convert(varchar(max),ch.comments), ''), '') 
		                  as 'content'
        -- select count(*) --138
        -- select top 100 *
        from bullhorn1.BH_CandidateHistory CH
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.candidateID = CH.candidateID 
        where C.isPrimaryOwner = 1 and C.candidateID is not null and ch.comments is not null --and cast(ch.comments as varchar(max)) <> '' 
UNION ALL
	SELECT --top 20 
	C.candidateID ,C.fullname  --, a.candidateuserID, C.userID
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
	SELECT --top 20 
	C.candidateID,C.fullname  --, a.candidateuserID, C.userID
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
                            + Coalesce('Opportunity: ' + NULLIF(cast(j2.title as varchar(max)), '') + char(10), '') --a.opportunityJobPostingID 
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
        select 
        C.candidateid, C.fullname
        , pl.dateadded 
	, Stuff( 'PLACEMENT: ' + char(10)
              + Coalesce('Billing Contact: ' + NULLIF(cast( concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email)  as varchar(max)), '') + char(10), '')  --pl.billingUserID
                     --+ Coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as varchar(max)), '') + char(10), '')
              + Coalesce('Bill Rate: ' + NULLIF(cast(pl.clientBillRate as varchar(max)), '') + char(10), '')
              + Coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as varchar(max)), '') + char(10), '')
              + Coalesce('Comments: ' + NULLIF(cast(pl.comments as varchar(max)), '') + char(10), '')
                     --+ Coalesce('Contract Employment Info: ' + NULLIF(cast(pl.contractInfoHeader as varchar(max)), '') + char(10), '')
              + Coalesce('Primary Timesheet Approver: ' + NULLIF(cast(pl.correlatedCustomText1 as varchar(max)), '') + char(10), '')
              + Coalesce('Secondary Timecard Approver: ' + NULLIF(cast(pl.correlatedCustomText2 as varchar(max)), '') + char(10), '')
              + Coalesce('Purchase Order Number: ' + NULLIF(cast(pl.correlatedCustomText3 as varchar(max)), '') + char(10), '')
              + Coalesce('Cost Center: ' + NULLIF(cast(pl.costCenter as varchar(max)), '') + char(10), '')
              + Coalesce('Insurance Reference: ' + NULLIF(cast(pl.customText1 as varchar(max)), '') + char(10), '')
              + Coalesce('Start Date: ' + NULLIF(cast(pl.dateBegin as varchar(max)), '') + char(10), '')
              + Coalesce('Effective Date (Client): ' + NULLIF(cast(pl.dateClientEffective as varchar(max)), '') + char(10), '')
              + Coalesce('Effective Date: ' + NULLIF(cast(pl.dateEffective as varchar(max)), '') + char(10), '')
              + Coalesce('Scheduled End: ' + NULLIF(cast(pl.dateEnd as varchar(max)), '') + char(10), '')
              + Coalesce('Days Guaranteed: ' + NULLIF(cast(pl.daysGuaranteed as varchar(max)), '') + char(10), '')
              + Coalesce('Days Pro-Rated: ' + NULLIF(cast(pl.daysProRated as varchar(max)), '') + char(10), '')
              + Coalesce('Employment Type: ' + NULLIF(cast(pl.employmentType as varchar(max)), '') + char(10), '')
              + Coalesce('Placement Fee (%): ' + NULLIF(cast(pl.fee as varchar(max)), '') + char(10), '')
              --+ Coalesce('Placement Fee (Flat): ' + NULLIF(cast(pl.flatFee as varchar(max)), '') + char(10), '')
              + Coalesce('Hours of Operation: ' + NULLIF(cast(pl.hoursOfOperation as varchar(max)), '') + char(10), '')
              + Coalesce('Hours Per Day: ' + NULLIF(cast(pl.hoursPerDay as varchar(max)), '') + char(10), '')
              + Coalesce('Rate Entry Type: ' + NULLIF(cast(pl.isMultirate as varchar(max)), '') + char(10), '')
              --+ Coalesce('Mark-up %: ' + NULLIF(cast(pl.markUpPercentage as varchar(max)), '') + char(10), '')
              + Coalesce('Over-time Pay Rate: ' + NULLIF(cast(pl.overtimeRate as varchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(pl.payRate as varchar(max)), '') + char(10), '')
                     --+ Coalesce('Pay Rate Information: ' + NULLIF(cast(pl.payRateInfoHeader as varchar(max)), '') + char(10), '')
                     --+ Coalesce('Permanent Employment Info: ' + NULLIF(cast(pl.permanentInfoHeader as varchar(max)), '') + char(10), '')
              + Coalesce('Referral Fee Type: ' + NULLIF(cast(pl.referralFeeType as varchar(max)), '') + char(10), '')
              + Coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as varchar(max)), '') + char(10), '')
              + Coalesce('Salary: ' + NULLIF(cast(pl.salary as varchar(max)), '') + char(10), '')
              + Coalesce('Pay Unit: ' + NULLIF(cast(pl.salaryUnit as varchar(max)), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(cast(pl.status as varchar(max)), '') + char(10), '')
        , 1, 0, '') as 'content'
        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid
UNION ALL
       -- Job Submission
        select 
        C.candidateid, C.fullname
        , jr.dateadded
	, Stuff(    'JOB SUBMISSION: ' + char(10)
                 --Coalesce('Appointments: ' + NULLIF(cast(jr.appointments as varchar(max)), '') + char(10), '')
              + Coalesce('Bill Rate: ' + NULLIF(cast(jr.billRate as varchar(max)), '') + char(10), '')
              + Coalesce('Comments: ' + NULLIF(cast(jr.comments as varchar(max)), '') + char(10), '')
              + Coalesce('Date Added: ' + NULLIF(cast(jr.dateAdded as varchar(max)), '') + char(10), '')
              --+ Coalesce('Date Last Modified: ' + NULLIF(cast(jr.dateLastModified as varchar(max)), '') + char(10), '')
              + Coalesce('Date Web Response: ' + NULLIF(cast(jr.dateWebResponse as varchar(max)), '') + char(10), '')
              + Coalesce('Job Order: ' + NULLIF(cast(j.title as varchar(max)), '') + char(10), '') --jr.jobPostingID
              + Coalesce('Latest Appointment: ' + NULLIF(cast(jr.latestAppointmentID as varchar(max)), '') + char(10), '')
              + Coalesce('Migrate GUID: ' + NULLIF(cast(jr.migrateGUID as varchar(max)), '') + char(10), '')
              --+ Coalesce('Owners: ' + NULLIF(cast(jr.owners as varchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(jr.payRate as varchar(max)), '') + char(10), '')
              + Coalesce('Salary: ' + NULLIF(cast(jr.salary as varchar(max)), '') + char(10), '')
              + Coalesce('Added By: ' + NULLIF(cast( concat(UC2.FirstName,' ',UC2.LastName,'     ',UC2.email) as varchar(max)), '') + char(10), '') --jr.sendingUserID
              + Coalesce('Source: ' + NULLIF(cast(jr.source as varchar(max)), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(cast(jr.status as varchar(max)), '') + char(10), '')
              --+ Coalesce('Task: ' + NULLIF(cast(jr.tasks as varchar(max)), '') + char(10), '')
              + Coalesce('Candidate: ' + NULLIF(cast( concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email) as varchar(max)), '') + char(10), '') --jr.userID 
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
                  , content
from comments where content <> '' 
--and candidateID = 2988 or fullname like '%Philip%'







/*
-- Add placement information under job posting
with placementnote as (
        --select jobPostingID
        select 
        C.candidateid
        , pl.dateadded 
	, Stuff(
	--, Stuff((select 
 Coalesce('Billing Contact: ' + NULLIF(cast( concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email)  as varchar(max)), '') + char(10), '')  --pl.billingUserID
       --+ Coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as varchar(max)), '') + char(10), '')
+ Coalesce('Bill Rate: ' + NULLIF(cast(pl.clientBillRate as varchar(max)), '') + char(10), '')
+ Coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as varchar(max)), '') + char(10), '')
+ Coalesce('Comments: ' + NULLIF(cast(pl.comments as varchar(max)), '') + char(10), '')
       --+ Coalesce('Contract Employment Info: ' + NULLIF(cast(pl.contractInfoHeader as varchar(max)), '') + char(10), '')
+ Coalesce('Primary Timesheet Approver: ' + NULLIF(cast(pl.correlatedCustomText1 as varchar(max)), '') + char(10), '')
+ Coalesce('Secondary Timecard Approver: ' + NULLIF(cast(pl.correlatedCustomText2 as varchar(max)), '') + char(10), '')
+ Coalesce('Purchase Order Number: ' + NULLIF(cast(pl.correlatedCustomText3 as varchar(max)), '') + char(10), '')
+ Coalesce('Cost Center: ' + NULLIF(cast(pl.costCenter as varchar(max)), '') + char(10), '')
+ Coalesce('Insurance Reference: ' + NULLIF(cast(pl.customText1 as varchar(max)), '') + char(10), '')
+ Coalesce('Start Date: ' + NULLIF(cast(pl.dateBegin as varchar(max)), '') + char(10), '')
+ Coalesce('Effective Date (Client): ' + NULLIF(cast(pl.dateClientEffective as varchar(max)), '') + char(10), '')
+ Coalesce('Effective Date: ' + NULLIF(cast(pl.dateEffective as varchar(max)), '') + char(10), '')
+ Coalesce('Scheduled End: ' + NULLIF(cast(pl.dateEnd as varchar(max)), '') + char(10), '')
+ Coalesce('Days Guaranteed: ' + NULLIF(cast(pl.daysGuaranteed as varchar(max)), '') + char(10), '')
+ Coalesce('Days Pro-Rated: ' + NULLIF(cast(pl.daysProRated as varchar(max)), '') + char(10), '')
+ Coalesce('Employment Type: ' + NULLIF(cast(pl.employmentType as varchar(max)), '') + char(10), '')
+ Coalesce('Placement Fee (%): ' + NULLIF(cast(pl.fee as varchar(max)), '') + char(10), '')
--+ Coalesce('Placement Fee (Flat): ' + NULLIF(cast(pl.flatFee as varchar(max)), '') + char(10), '')
+ Coalesce('Hours of Operation: ' + NULLIF(cast(pl.hoursOfOperation as varchar(max)), '') + char(10), '')
+ Coalesce('Hours Per Day: ' + NULLIF(cast(pl.hoursPerDay as varchar(max)), '') + char(10), '')
+ Coalesce('Rate Entry Type: ' + NULLIF(cast(pl.isMultirate as varchar(max)), '') + char(10), '')
--+ Coalesce('Mark-up %: ' + NULLIF(cast(pl.markUpPercentage as varchar(max)), '') + char(10), '')
+ Coalesce('Over-time Pay Rate: ' + NULLIF(cast(pl.overtimeRate as varchar(max)), '') + char(10), '')
+ Coalesce('Pay Rate: ' + NULLIF(cast(pl.payRate as varchar(max)), '') + char(10), '')
       --+ Coalesce('Pay Rate Information: ' + NULLIF(cast(pl.payRateInfoHeader as varchar(max)), '') + char(10), '')
       --+ Coalesce('Permanent Employment Info: ' + NULLIF(cast(pl.permanentInfoHeader as varchar(max)), '') + char(10), '')
+ Coalesce('Referral Fee Type: ' + NULLIF(cast(pl.referralFeeType as varchar(max)), '') + char(10), '')
+ Coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as varchar(max)), '') + char(10), '')
+ Coalesce('Salary: ' + NULLIF(cast(pl.salary as varchar(max)), '') + char(10), '')
+ Coalesce('Pay Unit: ' + NULLIF(cast(pl.salaryUnit as varchar(max)), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(cast(pl.status as varchar(max)), '') + char(10), '')
 , 1, 0, '') as note
	        -- select top 50 *
	        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid 	        
--               left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID
--               WHERE PL.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') as note
--        from bullhorn1.BH_Placement a group by a.jobPostingID 
        )
select count(*) from placementnote
--select * from placementnote where jobPostingID in (17,28,30,50,92,115)
--select jobPostingID from placementnote group by jobPostingID having count(*) > 1

*/

-- select * from bullhorn1.View_JobResponse where latestAppointmentID is not null;
