
with
  JPInfo as (
  	select JP.jobPostingID as JobID
  		, JP.title as JobTitle, jp.status as 'job_status'
		, Cl.clientID as ContactID, Cl.userID as ClientUserID, cl.status as 'contact_status'
		, UC.name as ContactName, UC.email as ContactEmail
		, CC.clientCorporationID as CompanyID, CC.name as CompanyName, CC.status as 'company_status'
	from bullhorn1.BH_JobPosting JP
	left join bullhorn1.BH_Client Cl on JP.clientUserID = Cl.userID
	left join bullhorn1.BH_UserContact UC on JP.clientUserID = UC.userID
	left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
	where 1=1 and JP.title <> '' and (JP.isdeleted <> 1 /*and JP.status <> 'Archive'*/)
	)
--select top 100 * from JPInfo order by JobID


select --top 200
         PL.jobPostingID as 'application-positionExternalId', pl.placementID
       , JPI.CompanyID, JPI.CompanyName, JPI.company_status, JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.contact_status, JPI.JobID, JPI.JobTitle, JPI.job_status, CAI.UserID, CAI.CandidateName as 'Candidate Name', CAI.candidate_status --, JR.userID, JPI.ClientUserID,
       , CAI.candidateID as 'application-candidateExternalId'
       --, 'OFFERED' as 'application-stage'
       
       --, pl.dateAdded as [application-actionedDate] --convert(varchar(10),PL.dateAdded,120)--as 'placed_date'
       , pl.dateBegin as 'start_date', DATEADD(year, 1, pl.dateBegin)  as 'to_date'
       , pl.dateEnd as 'Scheduled End Date'
       , pl.dateClientEffective as 'placement_date'
       , pl.dateeffective as 'placed_date'
       , case 
              when pl.employmentType in ('Permanent','Retained','Direct Hire') then 1
              --when pl.employmentType in ('Contract','Fixed Contract','Part-time','Fixed term Contract','Temp','Temp to Perm', 'Temporary') then 2
              else 2 end as 'position_type' --'Job Type'
       , case
              when pl.customText1 in ('EUR','Euro') then 'euro'
              when pl.customText1 in ('GBP','GBP £') then 'pound'
              when pl.customText1 in ('USD') then 'usd'
              when pl.customText1 in ('HKD','HK $') then 'hkd'
              else '' end as currency_type --**'singd'
                  
, '>>>Profit Split'
       , statement.email as 'user_email' -->> Profit split - %
       , 1 as 'profit_split_mode' -->> Profit split - %
       , pl.costcenter as 'Cost Center'

      
, '>>>PERMANENT'
       , pl.salary as 'contract_rate_from-MinimumSalary' --'gross_annual_salary'
       , 1 as 'use_quick_fee_forecast'
       , pl.fee*100 as 'percentage_of_annual_salary' --'Quick Fee Forecast % (Permanent Job)'
       , pl.salary*pl.fee as 'projected_profit'--, pl.salary, pl.fee
       --, pl.flatFee as 'N/A (Auto Calculated)'
       
, '>>>CONTRACT'
       , convert(float,pl.payRate) as 'pay_rate'
       , convert(float,pl.payRate) as 'total_pay_rate' --Pay rate + On-costs (number) = Total pay rate
       , 'chargeRate' as charge_rate_type
       , pl.clientBillRate as 'charge_rate'
       , pl.clientBillRate-convert(float,pl.payrate) as 'profit' --Charge rate - Total pay rate = Profit | Margin
       ,(pl.clientBillRate-convert(float,pl.payRate))*100/(case when pl.clientBillRate = 0 then 1 end) as 'margin_percent' --Margin % = Profit | Margin * 100 / Charge rate
       ,(pl.clientBillRate-convert(float,pl.payRate))*100/(case when convert(float,pl.payRate) = 0 then 1 end) as 'markup_percent' --Markup % = Profit | Margin * 100 / Total pay rate
       , case
              when pl.salaryUnit in ('Per Hour','Hourly','Per Hour,Per Hour') then 1
              when pl.salaryUnit in ('Per Day') then 2
              when pl.salaryUnit in ('Per Week') then 3
              when pl.salaryUnit in ('Per Month') then 4
              end as 'contract_rate_type'--, pl.salaryUnit    
       , Stuff(--'EMAIL: ' + char(10)
              + Coalesce('Placement ID: ' + NULLIF(convert(nvarchar(max), pl.placementid), '') + char(10), '') --as 'Contact External ID
              + Coalesce('Billing Contact: ' + NULLIF(convert(nvarchar(max), billing.name), '') + char(10), '') --as 'Contact External ID'
+ Coalesce('Source: ' + NULLIF(convert(nvarchar(max),pl.customText2), '') + char(10), '')
+ Coalesce('Job Location: ' + NULLIF(convert(nvarchar(max),pl.customText3), '') + char(10), '')
+ Coalesce('Fee Type: ' + NULLIF(convert(nvarchar(max),pl.customText4), '') + char(10), '')
+ Coalesce('Payment Received: ' + NULLIF(convert(nvarchar(max),pl.customTextBlock1), '') + char(10), '')
+ Coalesce('Guaranteed Period (Days): ' + NULLIF(convert(nvarchar(max),pl.daysGuaranteed), '') + char(10), '')
+ Coalesce('Referral Fee Type: ' + NULLIF(convert(nvarchar(max),pl.referralFeeType), '') + char(10), '')
              + Coalesce('Statement Contact: ' + NULLIF(convert(nvarchar(max), statement.name), '') + char(10), '') --as 'Contact External ID'-- Primary Timesheet Authoriser
+ Coalesce('Status: ' + NULLIF(convert(nvarchar(max),pl.status), '') + char(10), '')
--              + Coalesce('Ad hoc Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate6), '') + char(10), '')
--              + Coalesce('Ad hoc Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate6), '') + char(10), '')
--              + Coalesce('Annual Leave Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate7), '') + char(10), '')
--              + Coalesce('Annual Leave Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate7), '') + char(10), '')
--              + Coalesce('Bank Holiday Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate10), '') + char(10), '')
--              + Coalesce('Bank Holiday Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate10), '') + char(10), '')
--              + Coalesce('Billing Frequency: ' + NULLIF(convert(nvarchar(max),pl.billingFrequency), '') + char(10), '')
              + Coalesce('Comments: ' + NULLIF(convert(nvarchar(max), pl.comments), '') + char(10), '')
--              + Coalesce('Days Guaranteed: ' + NULLIF(convert(nvarchar(max), pl.daysguaranteed), '') + char(10), '')
--              + Coalesce('Days Pro-Rated: ' + NULLIF(convert(nvarchar(max),pl.daysProRated), '') + char(10), '')
--              + Coalesce('Employee Payment Type: ' + NULLIF(convert(nvarchar(max),pl.employeeType), '') + char(10), '')
--              + Coalesce('Employee Type: ' + NULLIF(convert(nvarchar(max), pl.employeetype), '') + char(10), '')
--              + Coalesce('Expenses (Ad hoc) Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate8), '') + char(10), '')
--              + Coalesce('Expenses (Ad hoc) Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate8), '') + char(10), '')
--              + Coalesce('Guarentee Information: ' + NULLIF(convert(nvarchar(max),pl.daysGuaranteed), '') + char(10), '')
--              + Coalesce('Hours Per Period: ' + NULLIF(convert(nvarchar(max),pl.hoursPerDay), '') + char(10), '')
--              + Coalesce('' + NULLIF(convert(nvarchar(max), pc.note), '') + char(10), '')
--              + Coalesce('On Call Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate9), '') + char(10), '')
--              + Coalesce('On Call Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate9), '') + char(10), '')
--              + Coalesce('Opt Out / In: ' + NULLIF(convert(nvarchar(max),pl.customDate2), '') + char(10), '')
--              + Coalesce('Overtime Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate2), '') + char(10), '')
--              + Coalesce('Overtime: ' + NULLIF(convert(nvarchar(max),pl.clientOverTimeRate), '') + char(10), '')
--              + Coalesce('Overtime: ' + NULLIF(convert(nvarchar(max),pl.overtimeRate), '') + char(10), '')
--              + Coalesce('Overtime Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate2), '') + char(10), '')
--              + Coalesce('Owner: ' + NULLIF(convert(nvarchar(max), statement.name), '') + char(10), '') --as 'Contact External ID'
--              + Coalesce('Pay Effective Date (Candidate): ' + NULLIF(convert(nvarchar(max),pl.dateEffective), '') + char(10), '')
--              + Coalesce('Payment Type: ' + NULLIF(convert(nvarchar(max),pl.employeeType), '') + char(10), '')
--              + Coalesce('Placement Fee (Flat): ' + NULLIF(convert(nvarchar(max),vp.flatFee), '') + char(10), '')
--              + Coalesce('Proof of Address: ' + NULLIF(convert(nvarchar(max),pl.customDate3), '') + char(10), '')
--              + Coalesce('Qualifications / Certification: ' + NULLIF(convert(nvarchar(max),pl.correlatedCustomDate2), '') + char(10), '')
--              + Coalesce('Rate Entry Type: ' + NULLIF(convert(nvarchar(max),pl.isMultirate), '') + char(10), '')
--              + Coalesce('Referral Fee Type: ' + NULLIF(convert(nvarchar(max),pl.referralFeeType), '') + char(10), '')
--              + Coalesce('Reporting to: ' + NULLIF(convert(nvarchar(max), pl.reportto), '') + char(10), '')
--              + Coalesce('Right To Work: ' + NULLIF(convert(nvarchar(max),pl.customDate1), '') + char(10), '')
--              + Coalesce('Saturday Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate3), '') + char(10), '')
--              + Coalesce('Saturday Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate3), '') + char(10), '')
--              + Coalesce('Standard Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate1), '') + char(10), '')
--              + Coalesce('Standard Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate1), '') + char(10), '')
--              + Coalesce('Status: ' + NULLIF(convert(nvarchar(max), pl.status), '') + char(10), '')
--              + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),pl.status), '') + char(10), '')
--              + Coalesce('Sunday Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate4), '') + char(10), '')
--              + Coalesce('Sunday Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate4), '') + char(10), '')
--              + Coalesce('Time and Expense: ' + NULLIF(convert(nvarchar(max),pl.timeAndExpense), '') + char(10), '')
--              + Coalesce('VAT %: ' + NULLIF(convert(nvarchar(max),pl.taxRate), '') + char(10), '')
--              + Coalesce('Weekend Overtime Charge Rate: ' + NULLIF(convert(nvarchar(max),pl.customBillRate5), '') + char(10), '')
--              + Coalesce('Weekend Overtime Pay Rate: ' + NULLIF(convert(nvarchar(max),pl.customPayRate5), '') + char(10), '')
       , 1, 0, '') as 'PlacementNote'
       , Stuff(
--               + Coalesce('Invoice contact: ' + NULLIF(convert(nvarchar(max), pl.reportto), '') + char(10), '')
--               + Coalesce('Billing Frequency: ' + NULLIF(convert(nvarchar(max), pl.billingFrequency), '') + char(10), '')
+ Coalesce('Fee Due Date: ' + NULLIF(convert(nvarchar(max),pl.customDate1), '') + char(10), '')
       , 1, 0, '') as 'InvoiceNote'
--        , '' as 'InvoiceNote'

--, pl.customText11 as 'Candidate > Candidate Company Number (LTD Company) Placement Details > Candidate Company Number'
--, pl.customText36 as 'Candidate > Candidate Company Name (LTD Company) Placement Details > Candidate Company Name'
--, pl.customText2 as 'Company Payment Terms'
--, pl.customText23 as 'Notice period (days)'
--, pl.customText28 as 'DOB'
--, pl.customText5 as 'Purchase order number'

, 3 as draft_offer --used to move OFFERED to PLACED in VC [offer]
, 2 as InvoiceStatus --used to update invoice status in VC [invoice] as 'active'
, 1 as renewal_index --default value in VC [invoice]
, 1 as renewal_flow_status
, -10 as Latest_user_id
, getdate() as Latest_update_date
, 0 as Tax_rate
, 'other' as Export_data_to
, 0 as Net_total
, 0 as Other_invoice_items_total
, 0 as Invoice_total
, 1 as 'compensation_period__period_type'
, 655 as 'offer_pay_charge__comp_setting_id'
, 3 as 'offer_pay_charge__tax_period_id'
, 1 as valid
, 1 as placed_valid
, 1 as invoice_valid
, 2 as position_type --Contract placements
, 1 as split_mode --1-percent 2-number
, 4 as contract_rate_type
--, 0 as contract_length
, case 
    when pl.dateEnd is not null and pl.dateEnd <> '' then cast(day(pl.dateEnd - pl.dateBegin) as decimal (9,2))/22
    when pl.dateEnd is null then 0
    end as contract_length
, 4 as contract_length_type
, 1 as pay_calculation_type
, 0 as use_time_temp --Time management: 0 manual - 1 timetemp - 2 astute payroll
, case when pl.hoursperday = 0 then 8 else pl.hoursperday end as working_hour_per_day
--, case when pl.hoursperday < 11 then pl.hoursperday else 8 end as working_hour_per_day
, 5 as working_day_per_week
, 40 as working_hour_per_week
, 22 as working_day_per_month
, 4 as working_week_per_month

-- select count(*) --2086-- select *  -- select distinct customText1 --,count(*) -- select distinct hoursperday
-- select distinct pl.employmentType -- select distinct pl.salaryUnit -- statement.email -- select DATEADD(year, 1, pl.dateBegin) 
from bullhorn1.BH_Placement PL --group by customText1 --where  pl.billingUserID <> pl.statementUserID
left join bullhorn1.View_Placement vp on vp.placementid = pl.placementid
left join (
       select placementID,  STRING_AGG( note, char(10)) WITHIN GROUP (ORDER BY note) note
       from ( 
              select pc.placementID,
                      + Stuff(
                      + Coalesce('Split: ' + NULLIF(convert(nvarchar(max), pc.commissionPercentage), '') + char(10), '')--, pc.commissionPercentage as 'Split %'
                      --+ Coalesce('Date Added: ' + NULLIF(convert(nvarchar(max), pc.dateAdded), '') + char(10), '')--, pc.dateAdded as 'N/A'
                      + Coalesce('Fee Split: ' + NULLIF(convert(nvarchar(max), pc.flatPayout), '') + char(10), '')--, pc.flatPayout as 'Split Amount'
                      + Coalesce('% of Gross Margin: ' + NULLIF(convert(nvarchar(max), pc.grossMarginPercentage), '') + char(10), '')--, pc.grossMarginPercentage as 'Margin % (Auto Calculated)'
                      --+ Coalesce('Placement: ' + NULLIF(convert(nvarchar(max), pc.placement), '') + char(10), '')
                      + Coalesce('Role: ' + NULLIF(convert(nvarchar(max), pc.role), '') + char(10), '')
                      + Coalesce('Status: ' + NULLIF(convert(nvarchar(max), pc.status), '') + char(10), '')
                      + Coalesce('Recipient: ' + NULLIF(convert(nvarchar(max), uc.name), '') + char(10), '') --pc.userid
                      , 1, 0, '') as note
              -- select * 
              from bullhorn1.BH_Commission pc
              left join (select userID,name from bullhorn1.BH_UserContact) UC on UC.UserID = pc.userID  
              /*where pc.placementID = 3*/ ) t
       GROUP BY placementID
       ) pc on pc.placementID = pl.placementID
left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail, CA.status as 'candidate_status' from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isdeleted <> 1 /*and CA.status <> 'Archive'*/) CAI on PL.userID = CAI.CandidateUserID 
left join ( select distinct userID, name, email from bullhorn1.BH_UserContact UC) billing on billing.userid = pl.billinguserid
left join ( select distinct userid, name, email from bullhorn1.BH_UserContact UC) statement on statement.userid = pl.statementUserID
left join JPInfo JPI on PL.jobPostingID = JPI.JobID
where JPI.JobID is not null and CAI.CandidateUserID is not null
--and pl.employmentType in ('Permanent','Direct Hire') and pl.salary <> 0
--and pl.employmentType in ('Contract','Fixed Contract','Part-time','Fixed term Contract','Temp','Temp to Perm', 'Temporary')
and PL.placementid in (3409, 3410, 3411, 3412, 3413, 3414, 3415, 3416, 3417, 3418, 3419)
--and JPI.JobID in (4529)
--and cai.CandidateUserID in (41914,73149,20656)
--and pl.clientBillRate = convert(float,pl.payRate)


--select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where ca.userid in (41914,73149,20656) and CA.isdeleted <> 1 and CA.status <> 'Archive'
--select * from bullhorn1.BH_Placement PL where PL.placementid in (3409, 3410, 3411, 3412, 3413, 3414, 3415, 3416, 3417, 3418, 3419)
