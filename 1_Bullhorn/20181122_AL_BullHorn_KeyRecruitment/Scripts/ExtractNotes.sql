drop table if exists VCCanNotes

;with

-- Web Responses
wr as (
        select jr.userid, jp.title,jr.status
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join bullhorn1.BH_JobPosting  jp on jp.jobPostingID = jr.jobPostingID )

, wr1 as (SELECT userID, STUFF((SELECT ', ' + concat('Title: ', [dbo].[ufn_RemoveForXMLUnsupportedCharacters](title),' - Status: ',status)  from wr WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM wr AS a GROUP BY a.userID )

-- PLACEMENT
, placementNotesTmp as (
	select 
	C.CandidateId
	,C.fullname
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
)

, placementNotes as (
	select
	CandidateId
	, string_agg([dbo].[fn_ConvertHTMLToText](content), char(10) + char(10)) as content
	from placementNotesTmp
	group by CandidateId
)

SELECT
		CA.candidateID as CanExtId
		, Stuff(
			Coalesce('ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')  
            + Coalesce('General Comments: ' + NULLIF(convert(varchar(max),CA.comments), '') + char(10), '')
			--+ Coalesce('Shift Availability: ' + NULLIF(cast(ca.CustomComponent3 as varchar(max)), '') + char(10), '')
			+ Coalesce('Notice Period: ' + NULLIF(cast(ca.customText4 as varchar(max)), '') + char(10), '')
			+ Coalesce('Monthly Salary: ' + NULLIF(cast(ca.customText5 as varchar(max)), '') + char(10), '')
			+ Coalesce('Current Benefits: ' + NULLIF(cast(ca.customText6 as varchar(max)), '') + char(10), '')
			+ Coalesce('Date Available: ' + NULLIF(cast(ca.dateAvailable as varchar(max)), '') + char(10), '')
			+ Coalesce('Available Until: ' + NULLIF(cast(ca.dateAvailableEnd as varchar(max)), '') + char(10), '')
			+ Coalesce('CV: ' + NULLIF(UW.description, '') , '')
			+ Coalesce('Employment Preference: ' + NULLIF(cast(ca.employmentPreference as varchar(max)), '') + char(10), '')
			+ Coalesce('Desired Hourly Rate: ' + NULLIF(cast(ca.hourlyRate as varchar(max)), '') + char(10), '')
            + Coalesce('Current Hourly Rate: ' + NULLIF(cast(ca.hourlyRateLow as varchar(max)), '') + char(10), '')
			+ Coalesce('Web Responses: ' + NULLIF(cast(wr1.name as varchar(max)), '') + char(10), '')
			+ Coalesce('Referred by: ' + NULLIF(convert(varchar(max),CA.referredBy), '') + char(10), '')
			+ Coalesce('Referred by User: ' + NULLIF(convert(varchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
			+ Coalesce('Status: ' + NULLIF(cast(ca.status as varchar(max)), '') + char(10), '')
			+ Coalesce('Willing to Relocate: ' + NULLIF( cast( iif(ca.willRelocate = 1, 'No', 'Yes') as varchar(max)), '') + char(10), '')
			+ Coalesce('Placements: ' + NULLIF(convert(varchar(max), pns.content), '') + char(10), '')


			--+ Coalesce('LTD Company Name: ' + NULLIF(cast(ca.customText1 as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company No.: ' + NULLIF(cast(ca.customText2 as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company Phone.: ' + NULLIF(cast(ca.customText3 as varchar(max)), '') + char(10), '')
   --         + Coalesce('Desired Daily Rate: ' + NULLIF(cast(ca.dayRate as varchar(max)), '') + char(10), '')
   --         + Coalesce('Current Daily Rate: ' + NULLIF(cast(ca.dayRateLow as varchar(max)), '') + char(10), '')
   --         + Coalesce('Employee Payment Type: ' + NULLIF(cast(ca.employeeType as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company Address 1: ' + NULLIF(cast(CA.secondaryAddress1 as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company Address 2: ' + NULLIF(cast(CA.secondaryAddress2 as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company City: ' + NULLIF(cast(CA.secondaryCity as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company County: ' + NULLIF(cast(CA.secondaryState as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company Post Code: ' + NULLIF(cast(CA.secondaryZip as varchar(max)), '') + char(10), '')
   --         + Coalesce('LTD Company Country: ' + NULLIF(cast(t.country as varchar(max)), '') + char(10), '') --CA.secondaryCountryID                     
   --         + Coalesce('Reference: ' + NULLIF(convert(varchar(max),r.note), '') + char(10), '')
                     
			, 1, 0, '') as Notes

into VCCanNotes

from bullhorn1.Candidate CA
left join wr1 on wr1.userid = CA.userid
left join placementNotes pns on CA.candidateID = pns.candidateID
left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
left join (SELECT userid, STUFF((
                        SELECT char(10) + NULLIF(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid
                        ) uw on uw.userid = ca.userid
where CA.isPrimaryOwner = 1
and ca.isDeleted = 0

select * from VCCanNotes