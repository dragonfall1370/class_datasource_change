with comments as (
        SELECT --top 1000
                  C.candidateID
				, C.fullname
                , UC.dateAdded
                        , stuff('*** COMMENT: ' + char(10)
                                + coalesce('Created Date: ' + NULLIF(convert(varchar,UC.dateAdded,120), '') + char(10), '')
                                + coalesce('Author: ' + NULLIF(U.name, '') + char(10), '')
                                + coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
								+ coalesce('About: ' + NULLIF(C.fullname, '') + char(10), '')
                                + coalesce('Comments: ' + NULLIF( 
                                          replace(replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.comments) --[dbo].[fn_ConvertHTMLToText](
                                          ,'Â','')
                                          ,'Â·','')
                                          ,'v\:* {behavior:url(#default#VML);}','')
                                          ,'o\:* {behavior:url(#default#VML);}','')
                                          ,'w\:* {behavior:url(#default#VML);}','')
                                          ,'.shape {behavior:url(#default#VML);}','') , '') + char(10), '')
                        , 1, 0, '') as 'content'
        from bullhorn1.BH_UserComment UC
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = UC.Userid
        left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
        where C.candidateID is not null --44836 rows

UNION ALL

        SELECT --top 1000
                  ch.candidateID ,C.fullname --,UC.Userid
                , ch.dateAdded
                , coalesce('*** HISTORY: ' + char(10)
		           + 'Date Added: ' + convert(varchar, ch.dateAdded, 120) + char(10) 
		           + 'Candidate History: ' + NULLIF(convert(nvarchar(max),ch.comments), ''), '') 
		           as 'content'
        from bullhorn1.BH_CandidateHistory CH
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate ) C on C.candidateID = CH.candidateID 
        where C.candidateID is not null and ch.comments is not null --and C.isPrimaryOwner = 1 and cast(ch.comments as varchar(max)) <> '' 

UNION ALL

	SELECT --top 20 
	C.candidateID ,C.fullname  --, a.candidateuserID, C.userID
	, a.dateAdded
	, concat_ws(char(10), '*** APPOINTMENT: '
						, coalesce('Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL) 
												+ coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,''), NULL), ''), NULL) --a.clientUserID 
						, coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(UC2.FirstName,'') + ' ', NULL) 
												+ coalesce(nullif(UC2.LastName,'') + ' - ', NULL) + nullif(UC2.email,''), NULL), ''), NULL) --a.candidateUserID 
						, coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), '') + char(10), '') --jobPostingID
						, coalesce('Communication Method: ' + NULLIF(convert(nvarchar(max),a.communicationMethod), '') + char(10), '')
						, coalesce('Date Begin: ' + NULLIF(convert(nvarchar(max), a.dateBegin, 120), ''), NULL)
						, coalesce('Date End: ' + NULLIF(convert(nvarchar(max),a.dateEnd, 120), ''), NULL)
						, coalesce('Type: ' + NULLIF(convert(nvarchar(max),a.type), '') + char(10), '') --CA.activePlacements
						, coalesce('Subject: ' + NULLIF(convert(nvarchar(max),a.subject), '') + char(10), '')
						, coalesce('Reminder: ' + NULLIF(convert(nvarchar(max),a.notificationMinutes), '') + char(10), '')
						, coalesce('Opportunity: ' + NULLIF(convert(nvarchar(max),j2.title), '') + char(10), '')
						, coalesce('Location: ' + NULLIF(convert(nvarchar(max),a.location), '') + char(10), '')
						, coalesce('Owner: ' + NULLIF(coalesce(coalesce(nullif(UC3.FirstName,'') + ' ', NULL)
											+ coalesce(nullif(UC3.LastName,'') + ' - ', NULL) + nullif(UC3.email,''), NULL), ''), NULL)
						, coalesce('Lead: ' + NULLIF(coalesce(coalesce(nullif(UC4.FirstName,'') + ' ', NULL)
											+ coalesce(nullif(UC4.LastName,'') + ' - ', NULL) + nullif(UC4.email,''), NULL), ''), NULL)
						, coalesce('File Name: ' + NULLIF(convert(nvarchar(max),af.name), '') + char(10), '')
						, coalesce('Description: ' + NULLIF( convert(nvarchar(max),a.description), '') + char(10), '')
				) as 'content'
        from bullhorn1.View_Appointment a
        left join bullhorn1.View_AppointmentFile af on af.appointmentID = a.appointmentID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = a.candidateuserID
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = a.ClientuserID
        left join bullhorn1.BH_UserContact UC2 ON UC2.userID = a.candidateUserID
        left join bullhorn1.BH_UserContact UC3 ON UC3.userID = a.userID
        left join bullhorn1.BH_UserContact UC4 ON UC4.userID = a.LeaduserID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_JobPosting j2 on j.jobPostingID = a.opportunityJobPostingID        
        where C.candidateID is not null --C.isPrimaryOwner = 1 --2680 rows

UNION ALL

	SELECT --top 20 
	C.candidateID, C.fullname  --, a.candidateuserID, C.userID
	, a.dateAdded
	, concat_ws(char(10), '*** TASK: '
						, coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(UC2.FirstName,'') + ' ', NULL) 
												+ coalesce(nullif(UC2.LastName,'') + ' - ', NULL) + nullif(UC2.email,''), NULL), ''), NULL) --a.candidateUserID 
						--, coalesce('Assigned To: ' + NULLIF(cast(a.childTaskOwners as nvarchar(max)), '') + char(10), '')
						, coalesce('Assigned To: ' + NULLIF(coalesce(coalesce(nullif(UC4.FirstName,'') + ' ', NULL)
												+ coalesce(nullif(UC4.LastName,'') + ' - ', NULL) + nullif(UC4.email,''), NULL), ''), NULL) --a.userID
						, coalesce('Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL)
												+ coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,''), NULL), ''), NULL) --a.clientUserID 
						, coalesce('Due Date And Time: ' + NULLIF(convert(nvarchar(max), a.dateBegin, 120), ''), NULL)
						, coalesce('Date End: ' + NULLIF(convert(nvarchar(max),a.dateEnd, 120), ''), NULL)
						, coalesce('Description: ' + NULLIF(cast( a.description as nvarchar(max)), '') + char(10), '') --[dbo].[fn_ConvertHTMLToText](
						--, coalesce('Visibility: ' + NULLIF(cast(a.isPrivate as nvarchar(max)), '') + char(10), '')
						, coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), '') + char(10), '') --jobPostingID
						--, coalesce('Lead: ' + NULLIF(cast( concat(UC3.FirstName,' ',UC3.LastName,'     ',UC3.email) as nvarchar(max)), '') + char(10), '') --a.leadUserID
						--, coalesce('Reminder: ' + NULLIF(cast(a.notificationMinutes as nvarchar(max)), '') + char(10), '')
						, coalesce('Opportunity: ' + NULLIF(cast(j2.title as nvarchar(max)), '') + char(10), '') --a.opportunityJobPostingID 
						, coalesce('Placement: ' + NULLIF(cast(a.placementID as nvarchar(max)), '') + char(10), '')
						, coalesce('Priority: ' + NULLIF(cast(a.priority as nvarchar(max)), '') + char(10), '')
						--, coalesce('Recurrence Day Bits: ' + NULLIF(cast(a.recurrenceDayBits as nvarchar(max)), '') + char(10), '')
						--, coalesce('Recurrence Frequency: ' + NULLIF(cast(a.recurrenceFrequency as nvarchar(max)), '') + char(10), '')
						--, coalesce('Recurrence Max: ' + NULLIF(cast(a.recurrenceMax as nvarchar(max)), '') + char(10), '')
						--, coalesce('Recurrence Month Bits: ' + NULLIF(cast(a.recurrenceMonthBits as nvarchar(max)), '') + char(10), '')
						--, coalesce('Recurrence Style: ' + NULLIF(cast(a.recurrenceStyle as nvarchar(max)), '') + char(10), '')
						--, coalesce('Recurrence Type: ' + NULLIF(cast(a.recurrenceType as nvarchar(max)), '') + char(10), '')
						, coalesce('Subject: ' + NULLIF(cast(a.subject as nvarchar(max)), '') + char(10), '')
						, coalesce('Type: ' + NULLIF(cast(a.type as nvarchar(max)), '') + char(10), '')
						, coalesce('Owner: ' + NULLIF(cast( concat(UC4.FirstName,' ',UC4.LastName,'     ',UC4.email) as nvarchar(max)), '') + char(10), '') --a.userID
				) as 'content'       
        from bullhorn1.View_Task a
        left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
        left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
        left join bullhorn1.BH_UserContact UC2 ON a.candidateUserID = UC2.userID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_UserContact UC3 ON a.leadUserID = UC3.userID
        left join bullhorn1.BH_UserContact UC4 ON a.userID = UC4.userID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid, isPrimaryOwner from bullhorn1.Candidate) C on C.userID = a.candidateuserID 
        left join bullhorn1.BH_JobPosting j2 on j2.jobPostingID = a.opportunityJobPostingID
        where C.candidateID is not null --C.isPrimaryOwner = 1 --


UNION ALL
       -- PLACEMENT
		select 
		C.candidateid, C.fullname
		, pl.dateadded 
		, stuff( '*** PLACEMENT: ' + char(10)
              + coalesce('Billing Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL)
										+ coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,'') + char(10), NULL), ''), NULL)  --pl.billingUserID
			--+ coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as nvarchar(max)), '') + char(10), '')
              + coalesce('Bill Rate: ' + NULLIF(cast(pl.clientBillRate as nvarchar(max)), '') + char(10), '')
              + coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as nvarchar(max)), '') + char(10), '')
              + coalesce('Comments: ' + NULLIF(cast(pl.comments as nvarchar(max)), '') + char(10), '')
			--+ coalesce('Contract Employment Info: ' + NULLIF(cast(pl.contractInfoHeader as nvarchar(max)), '') + char(10), '')
              + coalesce('Primary Timesheet Approver: ' + NULLIF(cast(pl.correlatedCustomText1 as nvarchar(max)), '') + char(10), '')
              + coalesce('Secondary Timecard Approver: ' + NULLIF(cast(pl.correlatedCustomText2 as nvarchar(max)), '') + char(10), '')
              + coalesce('Purchase Order Number: ' + NULLIF(cast(pl.correlatedCustomText3 as nvarchar(max)), '') + char(10), '')
              + coalesce('Cost Center: ' + NULLIF(cast(pl.costCenter as nvarchar(max)), '') + char(10), '')
              + coalesce('Insurance Reference: ' + NULLIF(cast(pl.customText1 as nvarchar(max)), '') + char(10), '')
              + coalesce('Start Date: ' + NULLIF(convert(nvarchar(10), pl.dateBegin, 120), '') + char(10), '')
              + coalesce('Effective Date (Client): ' + NULLIF(convert(nvarchar(10), pl.dateClientEffective, 120), '') + char(10), '')
              + coalesce('Effective Date: ' + NULLIF(convert(nvarchar(10), pl.dateEffective, 120), '') + char(10), '')
              + coalesce('Scheduled End: ' + NULLIF(convert(nvarchar(10), pl.dateEnd, 120), '') + char(10), '')
              + coalesce('Days Guaranteed: ' + NULLIF(cast(pl.daysGuaranteed as nvarchar(max)), '') + char(10), '')
              + coalesce('Days Pro-Rated: ' + NULLIF(cast(pl.daysProRated as nvarchar(max)), '') + char(10), '')
              + coalesce('Employment Type: ' + NULLIF(cast(pl.employmentType as nvarchar(max)), '') + char(10), '')
              + coalesce('Placement Fee (%): ' + NULLIF(cast(pl.fee as nvarchar(max)), '') + char(10), '')
			--+ coalesce('Placement Fee (Flat): ' + NULLIF(cast(pl.flatFee as nvarchar(max)), '') + char(10), '')
              + coalesce('Hours of Operation: ' + NULLIF(cast(pl.hoursOfOperation as nvarchar(max)), '') + char(10), '')
              + coalesce('Hours Per Day: ' + NULLIF(cast(pl.hoursPerDay as nvarchar(max)), '') + char(10), '')
              + coalesce('Rate Entry Type: ' + NULLIF(cast(pl.isMultirate as nvarchar(max)), '') + char(10), '')
			--+ coalesce('Mark-up %: ' + NULLIF(cast(pl.markUpPercentage as nvarchar(max)), '') + char(10), '')
              + coalesce('Over-time Pay Rate: ' + NULLIF(cast(pl.overtimeRate as nvarchar(max)), '') + char(10), '')
              + coalesce('Pay Rate: ' + NULLIF(cast(pl.payRate as nvarchar(max)), '') + char(10), '')
			--+ coalesce('Pay Rate Information: ' + NULLIF(cast(pl.payRateInfoHeader as nvarchar(max)), '') + char(10), '')
			--+ coalesce('Permanent Employment Info: ' + NULLIF(cast(pl.permanentInfoHeader as nvarchar(max)), '') + char(10), '')
              + coalesce('Referral Fee Type: ' + NULLIF(cast(pl.referralFeeType as nvarchar(max)), '') + char(10), '')
              + coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as nvarchar(max)), '') + char(10), '')
              + coalesce('Salary: ' + NULLIF(cast(pl.salary as nvarchar(max)), '') + char(10), '')
              + coalesce('Pay Unit: ' + NULLIF(cast(pl.salaryUnit as nvarchar(max)), '') + char(10), '')
              + coalesce('Status: ' + NULLIF(cast(pl.status as nvarchar(max)), '') + char(10), '')
        , 1, 0, '') as 'content'
        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid

UNION ALL
       -- Job Submission
			select 
			C.candidateid, C.fullname
			, jr.dateadded
			, stuff('*** JOB SUBMISSION: ' + char(10)
                 --coalesce('Appointments: ' + NULLIF(cast(jr.appointments as nvarchar(max)), '') + char(10), '')
              + coalesce('Bill Rate: ' + NULLIF(cast(jr.billRate as nvarchar(max)), '') + char(10), '')
              + coalesce('Comments: ' + NULLIF(cast(jr.comments as nvarchar(max)), '') + char(10), '')
              + coalesce('Date Added: ' + NULLIF(convert(nvarchar(10), jr.dateAdded, 120), '') + char(10), '')
              --+ coalesce('Date Last Modified: ' + NULLIF(cast(jr.dateLastModified as nvarchar(max)), '') + char(10), '')
              + coalesce('Date Web Response: ' + NULLIF(cast(jr.dateWebResponse as nvarchar(max)), '') + char(10), '')
              + coalesce('Job Order: ' + NULLIF(cast(j.title as nvarchar(max)), '') + char(10), '') --jr.jobPostingID
              + coalesce('Latest Appointment: ' + NULLIF(cast(jr.latestAppointmentID as nvarchar(max)), '') + char(10), '')
              + coalesce('Migrate GUID: ' + NULLIF(cast(jr.migrateGUID as nvarchar(max)), '') + char(10), '')
              --+ coalesce('Owners: ' + NULLIF(cast(jr.owners as nvarchar(max)), '') + char(10), '')
              + coalesce('Pay Rate: ' + NULLIF(cast(jr.payRate as nvarchar(max)), '') + char(10), '')
              + coalesce('Salary: ' + NULLIF(cast(jr.salary as nvarchar(max)), '') + char(10), '')
              + coalesce('Added By: ' + NULLIF(coalesce(coalesce(nullif(UC2.FirstName,'') + ' ', NULL)
								+ coalesce(nullif(UC2.LastName,'') + ' - ', NULL) + nullif(UC2.email,'') + char(10), NULL), ''), NULL) --jr.sendingUserID
              + coalesce('Source: ' + NULLIF(cast(jr.source as nvarchar(max)), '') + char(10), '')
              + coalesce('Status: ' + NULLIF(cast(jr.status as nvarchar(max)), '') + char(10), '')
              --+ coalesce('Task: ' + NULLIF(cast(jr.tasks as nvarchar(max)), '') + char(10), '')
              + coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL)
										+ coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,'') + char(10), NULL), ''), NULL) --jr.userID 
        , 1, 0, '') as note
        from bullhorn1.View_JobResponse jr
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = jr.userID
        left join bullhorn1.BH_UserContact UC2 ON UC2.userID = jr.sendingUserID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = jr.jobPostingID        
        left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = jr.userid
        --where jr.userid <> jr.sendingUserID
)

select --top 100
                   concat('PR', candidateID)
                  , -10 as 'user_account_id'
                  , 'comment' as 'category'
                  , 'candidate' as 'type'
                  , dateAdded as 'insert_timestamp'
                  , [dbo].[udf_StripHTML](content) as content
from comments where content <> '' --55563
order by candidateID