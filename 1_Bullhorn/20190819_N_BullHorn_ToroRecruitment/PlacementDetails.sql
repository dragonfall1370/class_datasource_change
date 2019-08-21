with ja as (
	select jr.jobPostingID
	, ca.candidateID
	, case when jp.employmentType is null then 301
		when JP.employmentType in ('Permanent','Opportunity','FT','Perm') then 301
		when JP.employmentType in ('Contract','Fixed Term','Temp','Temporary') then 302
		--when JP.employmentType in ('Temporary','Temp to Perm') then 303 --no more temp type
		else null end as JobType
	, case 
				when jr.status = '1st Interview' then 3
                when jr.status = '2nd Interview' then 4
                when jr.status = 'Candidate Rejected' then 1
                when jr.status = 'Client Rejected' then 2
                when jr.status = 'CV Sent' then 2
                when jr.status = 'Final Interview' then 4
                when jr.status = 'New Submission' then 1
                when jr.status = 'Offer Extended' then 5
                when jr.status = 'Offer Rejected' then 5
                when jr.status = 'Placed,Prescreen' then 6
                when jr.status = 'Shortlisted,Submitted' then 1
                else 0  
	end as stage
	, case when jr.status in ('Candidate Rejected', 'Client Rejected','Offer Rejected') then 'REJECTED'
		else NULL end as rejected
	, jr.status as substatus
	, jr.dateAdded
	, concat('JR', jr.jobResponseID) as referenceId --for updating placement purpose
	from bullhorn1.BH_JobResponse jr
	left join (select c.candidateID, c.userID as CandidateUserID, uc.userID, uc.name, uc.email as candidate_email
						from bullhorn1.Candidate c
						left join bullhorn1.BH_UserContact uc on c.userID = uc.userID) ca on ca.CandidateUserID = jr.userID
	left join bullhorn1.BH_JobPosting jp on jp.jobPostingID = jr.jobPostingID

	UNION ALL
--PLACEMENT
	select pl.jobPostingID
	, ca.candidateID
	, case when jp.employmentType is null then 301
		when JP.employmentType in ('Permanent','Opportunity','Perm','FT') then 301
		when JP.employmentType in ('Contract','Fixed Term','Temp','Temporary') then 302
		--when JP.employmentType in ('Temporary','Temp to Perm') then 303 --no more temp type
		else null end as JobType
	, 6 as stage
	, NULL as rejected
	, 'PLACED' as substatus
	, pl.dateAdded --can be used as placed date / offered date
	, concat('PL', pl.placementID) --for updating placement purpose
	from bullhorn1.BH_Placement pl
	left join (select c.candidateID, c.userID as CandidateUserID, uc.userID, uc.name, uc.email as candidate_email
						from bullhorn1.Candidate c
						left join bullhorn1.BH_UserContact uc on c.userID = uc.userID) ca on ca.CandidateUserID = pl.userID
	left join bullhorn1.BH_JobPosting jp on jp.jobPostingID = pl.jobPostingID
)

, higheststage as (select jobPostingID
		, candidateID
		, JobType
		, stage
		, rejected
		, substatus
		, dateAdded
		, row_number() over (partition by jobPostingID, candidateID order by stage desc, dateadded desc) as rn --highest stage first then latest date
		, referenceId
		from ja
		where stage > 0
)

select concat('TRJ', h.jobPostingID) as JobExtId
, concat('TRCD', candidateID) as CandidateExtId
, JobType as PlacedStage
, p.placementID
, p.dateBegin
, case 
		when Stage = 6 then 'OFFERED' --to be updated as PLACED afterward
		when Stage = 5 then 'OFFERED'
		when Stage = 4 then 'SECOND_INTERVIEW'
		when Stage = 3 then 'FIRST_INTERVIEW'
		when Stage = 2 then 'SENT'
		when Stage = 1 then 'SHORTLISTED'
		end as stage
, h.dateAdded
, case when rejected is not NULL then h.dateAdded
	else NULL end as rejected_date
, 0 as invoice_total
, dateBegin as StartDate
, dateClientEffective as PlacementDate --Placeme
, referenceId
, -10 as Latest_user_id
, getdate() as latest_update_date
, 3 as draft_offer --used to move OFFERED to PLACED in VC [offer]
, 2 as InvoiceStatus --used to update invoice status in VC [invoice] as 'active'
, 1 as renewal_index --default value in VC [invoice]
, 1 as renewal_flow_status
, 0 as tax_rate
, 'other' as export_data_to
, 0 as net_total
, 0 as other_invoice_items_totalntDate
, case when dateEffective is NULL then dateClientEffective
	else dateEffective end as PlacedDate--PlacedDate
, 1 as use_quick_fee_forecast
, case when fee = 0 then 100
	else fee * 100 end percentage_of_annual_salary --quick fee %
, coalesce(concat_ws(char(10)
--        , coalesce('Invoice Frequency: ' + nullif(convert(nvarchar(max),billingFrequency),''), NULL)
--	, coalesce('Invoicing Contact: ' + NULLIF(coalesce(coalesce(nullif(uc.FirstName,'') + ' ', NULL) + coalesce(nullif(uc.LastName,'') + ' - ', NULL) + nullif(uc.email,''), NULL), ''), NULL)
--	, coalesce('Invoice Rate: ' + nullif(convert(nvarchar(max),p.clientBillRate),''), NULL) --client charge rate | Charge Rate for contract
	, coalesce('Invoice Rate (Over-time): ' + nullif(convert(nvarchar(max),p.clientOvertimeRate),''), NULL)
	, coalesce('Comments: ' + nullif(convert(nvarchar(max),[bullhorn1].[fn_ConvertHTMLToText](comments)),''), NULL)
--	, coalesce('Start Date: ' + nullif(convert(nvarchar(max),p.dateBegin),''), NULL)
	, coalesce('Bill Effective Date (Client): ' + nullif(convert(nvarchar(max),p.dateClientEffective),''), NULL)
	, coalesce('Pay Effective Date (Candidate): ' + nullif(convert(nvarchar(max),p.dateEffective),''), NULL)
--	, coalesce('Scheduled End Date: ' + nullif(convert(nvarchar(max),p.dateEnd),''), NULL)
--	, coalesce('Employee Type: ' + nullif(p.employeeType,''), NULL)
--	, coalesce('Placement Fee (%): ' + nullif(convert(nvarchar(max),p.fee),''), NULL)
--	, coalesce('Mark-up %: ' + nullif(convert(nvarchar(max),jp.markupPercentage),''), NULL)
--	, coalesce('Pay Rate: ' + nullif(convert(nvarchar(max),p.payRate),''), NULL)
--	, coalesce('Salary: ' + nullif(convert(nvarchar(max),p.salary),''), NULL)
--	, coalesce('Referral Fee Type: ' + nullif(referralFeeType,''), NULL)
	), 'This offer/ placement details from data migration') as PlacementNote
, coalesce('Invoice Frequency: ' + nullif(convert(nvarchar(max),billingFrequency),''), NULL) as InvoiceNote,
        jp.markupPercentage*100 as markupPercentage,
        case when JobType=301 then p.salary else p.payRate end as Payrate,
		case when JobType=301 then p.salary*fee end as Perprofit,
--        jp.payRate as positioncontractPayrate,
        cast(case
                when p.salaryUnit='Per Hour' then '1'
                when p.salaryUnit='Per Day' then '2'
                when p.salaryUnit='Per Week' then '3'
                when p.salaryUnit='Per Month' then '4'         
                when p.salaryUnit='Per Annum' then '5'
                else null
        end as integer) as salaryUnit,
--        case
--                when jp.salaryUnit='Per Hour' then DATEDIFF(hour, dateBegin, p.dateEnd)
--                when jp.salaryUnit='Per Day' then DATEDIFF(day,dateBegin,p.dateEnd)
--                when jp.salaryUnit='Per Week' then DATEDIFF(week, dateBegin,p.dateEnd)
--                when jp.salaryUnit='Per Month' then DATEDIFF(month,dateBegin,p.dateEnd)       
--                when jp.salaryUnit='Per Annum' then DATEDIFF(year,dateBegin,p.dateEnd)
--                else null
--        end as contractlength,
        cast(case
                when p.salaryUnit='Per Hour' then '1'
                when p.salaryUnit='Per Day' then '2'           
                when p.salaryUnit='Per Week' then '3'
                when p.salaryUnit='Per Month' then '4'
                when p.salaryUnit='Per Annum' then '4'
                else null
        end as integer) as timeinterval,
        p.payRate*jp.markupPercentage as ProfitMargin,
        p.clientBillRate as Chargerate,
        case when (p.clientBillRate)<>0 and  (p.clientBillRate) is not null then (p.payRate*jp.markupPercentage)/(p.clientBillRate)*100  else 0 end as Margin
from higheststage h
left join bullhorn1.BH_Placement p on concat('PL',p.placementID) = h.referenceId
left join bullhorn1.BH_UserContact uc on p.billingUserID = uc.userID
left join bullhorn1.BH_JobPosting jp on jp.jobPostingID=h.jobPostingID
where rn = 1
and stage >= 5
and p.placementID is not NULL
and candidateID=15250
;