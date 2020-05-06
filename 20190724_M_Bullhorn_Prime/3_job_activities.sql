with comments as (
                select a.jobPostingID
					, UC.dateAdded
					, concat_ws(char(10)
						, '*** COMMENT:'
						, coalesce('Created Date: ' + NULLIF(convert(varchar,uc.dateAdded,120), ''), NULL)
						, coalesce('Author: ' + NULLIF(u.name, ''), NULL)
						, coalesce('Action: ' + NULLIF(uc.action, ''), NULL)
						, coalesce('Comments: ' + NULLIF(cast([bullhorn1].[fn_ConvertHTMLToText](uc.comments) as nvarchar(max)), ''), NULL)
					) as [content]
                from bullhorn1.BH_UserComment UC --where jobPostingID is not null
                left join bullhorn1.BH_JobPosting a on a.jobPostingID = UC.jobPostingID
                left join bullhorn1.BH_User U on U.userID = UC.commentingUserID
                where a.jobPostingID is not null

UNION ALL

	SELECT 
			a.jobPostingID
			, a.dateAdded
			, concat_ws(char(10), '*** APPOINTMENT: '
					, coalesce('Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL) + coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,''), NULL), ''), NULL) --a.clientUserID 
					, coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(UC2.FirstName,'') + ' ', NULL) + coalesce(nullif(UC2.LastName,'') + ' - ', NULL) + nullif(UC2.email,''), NULL), ''), NULL) --a.candidateUserID 
					, coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), ''), NULL) --jobPostingID
					, coalesce('Communication Method: ' + NULLIF(convert(nvarchar(max), a.communicationMethod), ''), NULL)
					, coalesce('Owner: ' + NULLIF(coalesce(coalesce(nullif(UC3.FirstName,'') + ' ', NULL) + coalesce(nullif(UC3.LastName,'') + ' - ', NULL) + nullif(UC3.email,''), NULL), ''), NULL)
					, coalesce('Lead: ' + NULLIF(coalesce(coalesce(nullif(UC4.FirstName,'') + ' ', NULL) + coalesce(nullif(UC4.LastName,'') + ' - ', NULL) + nullif(UC4.email,''), NULL), ''), NULL)
					, coalesce('Date Begin: ' + NULLIF(convert(nvarchar(max), a.dateBegin, 120), ''), NULL)
					, coalesce('Date End: ' + NULLIF(convert(nvarchar(max),a.dateEnd, 120), ''), NULL)
					, coalesce('Type: ' + NULLIF(convert(nvarchar(max),a.type), ''), NULL) --CA.activePlacements
					, coalesce('Subject: ' + NULLIF(convert(nvarchar(max),a.subject), ''), NULL)
					--, coalesce('Reminder: ' + NULLIF(convert(nvarchar(max),a.notificationMinutes), ''), NULL)
					, coalesce('Opportunity: ' + NULLIF(convert(nvarchar(max),j2.title), ''), NULL)
					, coalesce('Location: ' + NULLIF(convert(nvarchar(max),a.location), ''), NULL)
					, coalesce('File Name: ' + NULLIF(convert(nvarchar(max),af.name), ''), NULL)
					, coalesce(char(10) + 'Description: ' + [bullhorn1].[fn_ConvertHTMLToText](NULLIF(convert(nvarchar(max),a.description), '')), NULL)
				) as [content]
        from bullhorn1.View_Appointment a
        left join bullhorn1.View_AppointmentFile af on af.appointmentID = a.appointmentID
        left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = Cl.userID
        left join bullhorn1.BH_UserContact UC2 ON UC2.userID = a.candidateUserID
        left join bullhorn1.BH_UserContact UC3 ON UC3.userID = a.userID
        left join bullhorn1.BH_UserContact UC4 ON UC4.userID = a.LeaduserID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_JobPosting j2 on j.jobPostingID = a.opportunityJobPostingID
        where j.jobPostingID is not null --23 rows
		 
UNION ALL

	SELECT ---top 20 
		j.jobPostingID as 'externalid'
		, a.dateAdded
				, concat_ws(char(10), '*** TASK: '
						, coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(UC2.FirstName,'') + ' ', NULL) + coalesce(nullif(UC2.LastName,'') + ' - ', NULL) + nullif(UC2.email,''), NULL), ''), NULL) --a.candidateUserID 
						--, coalesce('Assigned To: ' + NULLIF(cast(a.childTaskOwners as nvarchar(max)), ''), NULL)
						, coalesce('Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL) + coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,''), NULL), ''), NULL) --a.clientUserID 
						, coalesce('Due Date And Time: ' + NULLIF(convert(nvarchar(max), a.dateBegin, 120), ''), NULL)
						, coalesce('Date End: ' + NULLIF(convert(nvarchar(max),a.dateEnd, 120), ''), NULL)
						, coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), ''), NULL) --jobPostingID
						--, coalesce('Reminder: ' + NULLIF(cast(a.notificationMinutes as nvarchar(max)), ''), NULL)
						, coalesce('Opportunity: ' + NULLIF(cast(a.opportunityJobPostingID as nvarchar(max)), ''), NULL)
						, coalesce('Placement: ' + NULLIF(cast(a.placementID as nvarchar(max)), ''), NULL)
						, coalesce('Priority: ' + NULLIF(cast(a.priority as nvarchar(max)), ''), NULL)
						--, coalesce('Recurrence Day Bits: ' + NULLIF(cast(a.recurrenceDayBits as varchar(max)), ''), NULL)
						--, coalesce('Recurrence Frequency: ' + NULLIF(cast(a.recurrenceFrequency as varchar(max)), ''), NULL)
						--, coalesce('Recurrence Max: ' + NULLIF(cast(a.recurrenceMax as varchar(max)), ''), NULL)
						--, coalesce('Recurrence Month Bits: ' + NULLIF(cast(a.recurrenceMonthBits as varchar(max)), ''), NULL)
						--, coalesce('Recurrence Style: ' + NULLIF(cast(a.recurrenceStyle as varchar(max)), ''), NULL)
						--, coalesce('Recurrence Type: ' + NULLIF(cast(a.recurrenceType as varchar(max)), ''), NULL)
						--, coalesce('Visibility: ' + NULLIF(cast(a.isPrivate as nvarchar(max)), ''), NULL)
						, coalesce('Subject: ' + NULLIF(cast(a.subject as nvarchar(max)), ''), NULL)
						, coalesce('Type: ' + NULLIF(cast(a.type as nvarchar(max)), ''), NULL)
						, coalesce('Owner: ' + NULLIF(coalesce(coalesce(nullif(UC4.FirstName,'') + ' ', NULL) + coalesce(nullif(UC4.LastName,'') + ' - ', NULL) + nullif(UC4.email,''), NULL), ''), NULL) --a.userID
						, coalesce(char(10) + 'Description: ' + [bullhorn1].[fn_ConvertHTMLToText](NULLIF(convert(nvarchar(max),a.description), '')), NULL)
				) as [content] 
        from bullhorn1.View_Task a
        left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
        left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
        left join bullhorn1.BH_UserContact UC2 ON a.candidateUserID = UC2.userID
        left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
        left join bullhorn1.BH_UserContact UC3 ON a.leadUserID = UC3.userID
        left join bullhorn1.BH_UserContact UC4 ON a.userID = UC4.userID
        where j.jobPostingID is not null --5 rows

UNION ALL
        -- PLACEMENT
        select 
        PL.jobPostingID as 'externalid' 
        , pl.dateadded 
		, Stuff( '*** PLACEMENT: ' + char(10)
              + coalesce('Billing Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL) + coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,''), NULL), ''), NULL)  --pl.billingUserID
			--+ coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as varchar(max)), '') + char(10), '')
              + coalesce('Bill Rate: ' + NULLIF(cast(pl.clientBillRate as varchar(max)), '') + char(10), '')
              + coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as varchar(max)), '') + char(10), '')
              + coalesce('Comments: ' + NULLIF(cast(pl.comments as varchar(max)), '') + char(10), '')
			--+ coalesce('Contract Employment Info: ' + NULLIF(cast(pl.contractInfoHeader as varchar(max)), '') + char(10), '')
              + coalesce('Primary Timesheet Approver: ' + NULLIF(cast(pl.correlatedCustomText1 as varchar(max)), '') + char(10), '')
              + coalesce('Secondary Timecard Approver: ' + NULLIF(cast(pl.correlatedCustomText2 as varchar(max)), '') + char(10), '')
              + coalesce('Purchase Order Number: ' + NULLIF(cast(pl.correlatedCustomText3 as varchar(max)), '') + char(10), '')
              + coalesce('Cost Center: ' + NULLIF(cast(pl.costCenter as varchar(max)), '') + char(10), '')
              + coalesce('Insurance Reference: ' + NULLIF(cast(pl.customText1 as varchar(max)), '') + char(10), '')
              + coalesce('Start Date: ' + NULLIF(convert(nvarchar(max),pl.dateBegin,120), '') + char(10), '')
              + coalesce('Effective Date (Client): ' + NULLIF(convert(nvarchar(max),pl.dateClientEffective, 120), '') + char(10), '')
              + coalesce('Effective Date: ' + NULLIF(convert(nvarchar(max),pl.dateEffective, 120), '') + char(10), '')
              + coalesce('Scheduled End: ' + NULLIF(convert(nvarchar(max), pl.dateEnd, 120), '') + char(10), '')
              + coalesce('Days Guaranteed: ' + NULLIF(cast(pl.daysGuaranteed as varchar(max)), '') + char(10), '')
              + coalesce('Days Pro-Rated: ' + NULLIF(cast(pl.daysProRated as varchar(max)), '') + char(10), '')
              + coalesce('Employment Type: ' + NULLIF(cast(pl.employmentType as varchar(max)), '') + char(10), '')
              + coalesce('Placement Fee (%): ' + NULLIF(cast(pl.fee as varchar(max)), '') + char(10), '')
			--+ coalesce('Placement Fee (Flat): ' + NULLIF(cast(pl.flatFee as varchar(max)), '') + char(10), '')
              + coalesce('Hours of Operation: ' + NULLIF(cast(pl.hoursOfOperation as varchar(max)), '') + char(10), '')
              + coalesce('Hours Per Day: ' + NULLIF(cast(pl.hoursPerDay as varchar(max)), '') + char(10), '')
              + coalesce('Rate Entry Type: ' + NULLIF(cast(pl.isMultirate as varchar(max)), '') + char(10), '')
			--+ coalesce('Mark-up %: ' + NULLIF(cast(pl.markUpPercentage as varchar(max)), '') + char(10), '')
              + coalesce('Over-time Pay Rate: ' + NULLIF(cast(pl.overtimeRate as varchar(max)), '') + char(10), '')
              + coalesce('Pay Rate: ' + NULLIF(cast(pl.payRate as varchar(max)), '') + char(10), '')
			--+ coalesce('Pay Rate Information: ' + NULLIF(cast(pl.payRateInfoHeader as varchar(max)), '') + char(10), '')
			--+ coalesce('Permanent Employment Info: ' + NULLIF(cast(pl.permanentInfoHeader as varchar(max)), '') + char(10), '')
              + coalesce('Referral Fee Type: ' + NULLIF(cast(pl.referralFeeType as varchar(max)), '') + char(10), '')
              + coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as varchar(max)), '') + char(10), '')
              + coalesce('Salary: ' + NULLIF(cast(pl.salary as varchar(max)), '') + char(10), '')
              + coalesce('Pay Unit: ' + NULLIF(cast(pl.salaryUnit as varchar(max)), '') + char(10), '')
              + coalesce('Status: ' + NULLIF(cast(pl.status as varchar(max)), '') + char(10), '')
        , 1, 0, '') as 'content'
        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
        left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
        left join (select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid --106 rows
)

select --top 100
                   concat('PR', jobPostingID) as JobExtId
                  , -10 as 'user_account_id'
                  , 'comment' as 'category'
                  , 'job' as 'type'
                  , dateAdded as 'insert_timestamp'
                  , content as 'content'
from comments where 'content' <> '' --170 rows