--PART 1
---REFERENCE CHECK ON SIRIUS DELTA DATA --22 Feb 2018
select * from CompanyDelta
where ID in (select ID from Company)

select * from ContactDelta
where ID in (select ID from Contact) --ID: 0039000002P3c0FAAR (1 row)
-----
select * from Contact
where ID = '0039000002P3c0FAAR'

UNION ALL

select * from ContactDelta
where ID = '0039000002P3c0FAAR'
-----
select * from CandidateDelta
where ID in (select ID from Candidate) --94 rows

-----
select * from JobsDelta
where ID in (select ID from Jobs) --ID: a0C9000000lHbnEEAS (1 row)

-----
select * from CompanyDelta
where NAME in (select NAME from Company) --00190000020xIacAAE

select * from Company
where NAME = 'Myadvisor.ai' --0019000001oF8dqAAC


--PART 2
---2.1. COMPANY ACTIVITIES
select t.ACCOUNTID as CompanyExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('Subject: ', t.SUBJECT),char(10)
		 + concat('Status: ', t.STATUS,char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Short description: ', t.SHORT_DESCRIPTION__C,char(10))
		 + concat('*** Completed date: ', convert(varchar(20),t.COMPLETED_DATE__C,120))
		 ) as Sirius_company_activity
	, getdate() as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'company' as Sirius_type
	from TasksDelta t
	left join SiriusUsers su on su.ID = t.OWNERID
	where t.ACCOUNTID is not NULL

--Total: 19077 rows (169 company delta + 18908 existing company)
	
	
---2.2. CONTACT ACTIVITIES
--CONTACT ACTIVITIES (SMS HISTORY)
select sh.SMAGICINTERACT__CONTACT__C as ContactExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('SMS name: ', sh.NAME),char(10)
		 + concat('Created date: ', convert(varchar(20),sh.CREATEDDATE,120),char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Interact Name: ', sh.SMAGICINTERACT__NAME__C,char(10))
		 + concat('*** SMS Text: ', sh.SMAGICINTERACT__SMSTEXT__C)
		 ) as Sirius_comments
	, CONVERT(varchar(20), sh.CREATEDDATE, 120) as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'contact' as Sirius_type
	from SMSHistoryDelta sh
	left join SiriusUsers su on su.ID = sh.OWNERID
	where sh.SMAGICINTERACT__CONTACT__C is not NULL
	
--total: 501 rows (0 contact delta + 501 existing contact)

--CONTACT TASKS
select t.WHOID as ContactExtID
	, -10 as Sirius_user_account_id
	, left(t.SUBJECT,200) as Sirius_subject
	, Concat(concat('Subject: ', t.SUBJECT),char(10)
		 + concat('Status: ', t.STATUS,char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Short description: ', t.SHORT_DESCRIPTION__C,char(10))
		 + case when t.COMPLETED_DATE__C is NULL then 'No completed date'
		 else concat('*** Completed date: ', convert(varchar(20),t.COMPLETED_DATE__C,103)) end
		 ) as Sirius_contact_tasks
	, t.COMPLETED_DATE__C
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 120)
	else getdate() end as Sirius_insert_timestamp
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 120) 
	else getdate() + 3 end as Sirius_next_contact_date
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 120) 
	else getdate() + 3 end as Sirius_next_contact_to_date
	, 'Australia/Sydney' as Sirius_time_zone
	, 'task' as Sirius_category
	, 'contact' as Sirius_type
	from TasksDelta t
	left join SiriusUsers su on su.ID = t.OWNERID
	where t.WHOID is not NULL

--Total: 19087 rows (455 delta contacts + 18632 existing contact)

	
---2.3. CANDIDATE ACTIVITIES
--CANDIDATE ACTIVITIES (SMS HISTORY)
select sh.SMAGICINTERACT__CONTACT__C as CandidateExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('SMS name: ', sh.NAME),char(10)
		 + concat('Created date: ', convert(varchar(20),sh.CREATEDDATE,120),char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Interact Name: ', sh.SMAGICINTERACT__NAME__C,char(10))
		 + concat('SMS Text: ', sh.SMAGICINTERACT__SMSTEXT__C)
		 ) as Sirius_SMS_content
	, case when sh.CREATEDDATE is not NULL then convert(varchar(30), replace(replace(sh.CREATEDDATE,'T',' '),'.000Z',''), 120)
	else getdate() end as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'candidate' as Sirius_type
	from SMSHistoryDelta sh
	left join SiriusUsers su on su.ID = sh.OWNERID
	where sh.SMAGICINTERACT__CONTACT__C is not NULL
	
--Total: 501 (81 delta candidates + 418 existing candidates)

--CANDIDATE TASKS
select t.WHOID as CandidateExtID
	, -10 as Sirius_user_account_id
	, left(t.SUBJECT,200) as Sirius_subject
	, Concat(concat('Subject: ', t.SUBJECT),char(10)
		 + concat('Status: ', t.STATUS,char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Short description: ', t.SHORT_DESCRIPTION__C,char(10))
		 + case when t.COMPLETED_DATE__C is NULL then 'No completed date'
		 else concat('*** Completed date: ', convert(varchar(20),t.COMPLETED_DATE__C,103)) end
		 ) as Sirius_candidate_tasks
	, t.COMPLETED_DATE__C
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 120)
	else getdate() end as Sirius_insert_timestamp
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 120) 
	else getdate() + 3 end as Sirius_next_contact_date
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 120) 
	else getdate() + 3 end as Sirius_next_contact_to_date
	, 'Australia/Sydney' as Sirius_time_zone
	, 'task' as Sirius_category
	, 'candidate' as Sirius_type
	from TasksDelta t
	left join SiriusUsers su on su.ID = t.OWNERID
	where t.WHOID is not NULL and t.WHOID in (select ID from CandidateDelta where PEOPLECLOUD1__STATUS__C not in ('Inactive'))
	
--2.4. JOB PLACEMENTS ACTIVITIES

select * from activity_job --3798 rows

--->> REMOVE INCORRECT PLACEMENTS INFO IN VINCERE
select * from activity
where type = 'job'
and position_id > 0
and content like 'Job Application ID%' --2654 rows


select * from activity_job
where exists (select id from activity
where activity.id = activity_job.activity_id
and type = 'job'
and position_id > 0
and content like 'Job Application ID%') --2654 rows

--->> UPDATE NEW PLACEMENTS ON JOBS
select cm.PEOPLECLOUD1__PLACEMENT__C as Sirius_jobExtID
	, -10 as Sirius_user_account_id
	, concat('Job Application ID: ',cm.ID,char(10)
		, 'Candidate name: ',c.FIRSTNAME, ' ', c.LASTNAME,char(10)
		, 'Created date: ',c.CREATEDDATE,char(10)
		, coalesce('Fee based on: ' + cm.FEE_BASED_ON__C + char(10),'')
		, coalesce('Pro rata months: ' + convert(varchar(max),cm.PRO_RATA_MONTHS__C) + char(10),'')
		, coalesce('Base salary: ' + convert(varchar(max),cm.BASE_SALARY__C) + char(10),'')
		, coalesce('Super: ' + cm.SUPER__C + char(10),'')
		, coalesce('Fee: ' + cm.FEE__C + char(10),'')
		, coalesce('Flat fee: ' + cm.FLAT_FEE__C + char(10),'')
		, coalesce('Actual placement value: ' + cm.ACTUAL_PLACEMENT_VALUE__C + char(10),'')
		, coalesce('Pay rate custom: ' + cm.PAY_RATE_CUSTOM__C + char(10),'')
		, coalesce('Change rate custom: ' + cm.CHARGE_RATE_CUSTOM__C + char(10),'')
		, coalesce('Oncost: ' + cm.ONCOST__C + char(10),'')
		, coalesce('Oncost value: ' + cm.ONCOST_VALUE__C + char(10),'')
		, coalesce('Margin value: ' + cm.MARGIN_VALUE__C + char(10),'')
		, coalesce('Margin: ' + cm.MARGIN__C + char(10),'')
		, coalesce('Hours: ' + cm.HOURS__C + char(10),'')
		, coalesce('Client charge: ' + cm.CLIENT_CHARGE__C + char(10),'')
		, coalesce('Candidate pay: ' + cm.CANDIDATE_PAY__C + char(10),'')
		, coalesce('Hours of work: ' + cm.HOURS_OF_WORK__C + char(10),'')
		, coalesce('Assignment location: ' + cm.ASSIGNMENT_LOCATION__C + char(10),'')
		, coalesce('Guarantee Period: ' + cm.GUARANTEE_PERIOD__C + char(10),'')
		, coalesce('Guarantee date: ' + cm.GUARANTEE_DATE__C + char(10),'')
		, coalesce('Placement type: ' + cm.PLACEMENT_TYPE__C + char(10),'')
		, coalesce('PO Number: ' + cm.PO_NUMBER__C + char(10),'')
		, coalesce('Invoice Number: ' + cm.INVOICE_NUMBER__C + char(10),'')
		, coalesce('Due date: ' + convert(varchar(20),cm.DUE_DATE__C,120) + char(10),'')
		, coalesce('Placed date: ' + convert(varchar(20),cm.PLACED_DATE__C,120) + char(10),'')
		, coalesce('Weekly margin Sirius: ' + cm.WEEKLY_MARGIN_SIRIUS__C + char(10),'')
		, coalesce('Total package: ' + convert(varchar(max),cm.TOTAL_PACKAGE_C__C) + char(10),'')
		, coalesce('Weekly margin IND SBS: ' + cm.WEEKLY_MARGIN_IND_SBS__C + char(10),'')
		, coalesce('Calculated total package: ' + cm.CALCULATED_TOTAL_PACKAGE__C + char(10),'')
		, coalesce('Candidate payment type 1: ' + cm.CANDIDATE_PAYMENT_TYPE1__C + char(10),'')
		, coalesce('ABN ACN Company Enterprise: ' + cm.ABN_ACN_COMPANY_ENTERPRISE__C + char(10),'')
		, coalesce('Address Company Enterprise: ' + cm.ADDRESS_COMPANY_ENTERPRISE__C + char(10),'')
		, coalesce('Company Enterprise name: ' + cm.COMPANY_ENTERPRISE_NAME__C + char(10),'')
		, coalesce('Payment terms: ' + cm.PAYMENT_TERMS__C + char(10),'')
		, coalesce('ABN ACN: ' + cm.ABN_ACN__C + char(10),'')
		, coalesce('Client signatory email: ' + cm.CLIENT_SIGNATORY_EMAIL__C + char(10),'')
		, coalesce('Company address: ' + cm.COMPANY_ADDRESS__C + char(10),'')
		, coalesce('Notice period: ' + cm.NOTICE_PERIOD__C + char(10),'')
		, coalesce('Invoice recipient Email address: ' + cm.INVOICE_RECIPIENT_EMAIL_ADDRESS__C + char(10),'')
		, coalesce('Status candidate progress: ' + cm.STATUS_CANDIDATE_PROGRESS__C + char(10),'')
		, coalesce('Total fee: ' + cm.TOTAL_FEE__C + char(10),'')
		, coalesce('Record type name: ' + cm.RECORD_TYPE_NAME__C + char(10),'')
		, coalesce('Monthly drip fee: ' + cm.MONTHLY_DRIP_FEE__C + char(10),'')
		, coalesce('Sales value: ' + cm.SALES_VALUE__C,'')
		) as Sirius_content
	, convert(datetime, convert(varchar(30), replace(replace(cm.CREATEDDATE,'T',' '),'.000Z','')), 101) as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'job' as Sirius_type
from CandidateManagementDelta cm
left join Candidate c on c.ID = cm.PEOPLECLOUD1__CANDIDATE__C
where STATUS_CANDIDATE_PROGRESS__C like '%Placed%'
	
--2.5. PLACEMENTS START / END DATE 
---Table input:

---Mapping:
select * from position_candidate where candidate_id = 420893 and position_description_id = 100233; --41929

select opi.*
from offer_personal_info opi
left join offer o on o.id = opi.offer_id
where o.position_candidate_id = 41929;