--select distinct ts2_app_status_c from ts2_application_c a;
--select distinct ts2_application_status_c from ts2_application_c a;
--select distinct ts2_status_c from ts2_application_c a;
--select distinct ts2extams_substatus_c from ts2_application_c a;
--select id, concat(firstname,' ',lastname) as fullname, email from contact where id in ('0031n00001eSArWAAW')

with
ja0 as (
select   a.id as appid, a."name" as "JobApplicationName"
       , j.company_id, j.company_name
       , j.contact_id, j.contact_fullname
       , j.job_id, j.job_number, j.job_title
       , case when rt.name is null then 301 --'PERMANENT'
              when rt.name in ('Perm') then 301 --'PERMANENT'
              when rt.name in ('Contract do not use','Fixed Term Contract','Temp','Temp-to-Perm') then 302 --'CONTRACT'
              when rt.name in ('Closed') and j.ts2_date_filled_c is null then 301 --'PERMANENT'
              when rt.name in ('Closed') and j.ts2_date_filled_c is not null then 302 --'CONTRACT'
              end as jobtype
       , can.id as can_id, can.fullname as "candidate_fullname"
       , a.CreatedDate
       , case
            when a.ts2extams_substatus_c = 'Candidate Rejected' then 'OFFERED'-- > Rejected'
            when a.ts2extams_substatus_c = 'Candidate Sent' then 'SENT'-- > Pending'
            when a.ts2extams_substatus_c = 'Drop Out' then 'PLACED' --'PLACED > Rejected'
            when a.ts2extams_substatus_c = 'F2F' then 'FIRST_INTERVIEW'-- > Pending Sub Status > F2F'
            when a.ts2extams_substatus_c = 'Final' then 'SECOND_INTERVIEW'-- > Pending Sub Status > Final'
            --when a.ts2extams_substatus_c = 'New' then 'CANDIDATES'-- > Pending'
            when a.ts2extams_substatus_c = 'Offer Made' then 'OFFERED'
            when a.ts2extams_substatus_c = 'Placed' then 'PLACED' --'PLACED'
            when a.ts2extams_substatus_c = 'Rebate' then 'PLACED' --'PLACED Sub Status > Rebate'
            when a.ts2extams_substatus_c = 'Review' then 'SHORTLISTED'-- > Pending'
            when a.ts2extams_substatus_c = 'Send' then 'SHORTLISTED'-- > Pending'
            when a.ts2extams_substatus_c = 'Tel / Skype' then 'FIRST_INTERVIEW'-- > Pending Sub Status > Tel / Skype'
            when a.ts2extams_substatus_c = 'Test' then 'FIRST_INTERVIEW'-- > Pending Sub Status > Test'
            when a.ts2extams_substatus_c = 'Withdrawal' then 'FIRST_INTERVIEW'-- > Rejected'
            end as appstage -- SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT
		, a.ts2extams_substatus_c as sub_status
		, case when a.ts2extams_substatus_c in ('Withdrawal', 'Candidate Rejected', 'Drop Out') then lastmodifieddate::date
			else NULL end as rejected_date
from ts2_application_c a
left join (select left(id,15) as id, name from recordtype) rt on rt.id = a.recordtypeid
left join (select job.id as "job_id", job.ts2_job_number_c as "job_number"
				, job.name as "job_title", ts2_date_filled_c
                , con.id as "contact_id", con.fullname as "contact_fullname"
                , com.id as "company_id", com.companyname as "company_name"
				from ts2_job_c job --JOB
				left join ( select id, concat(firstname,' ',lastname) as fullname, email, title 
							from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = job.ts2_contact_c --CONTACT
				left join ( select id, name as "companyname" from account) com ON com.id = job.ts2_account_c --COMPANY
				) j on j.job_id = a.ts2_job_c --JOB reference
left join (select id, concat(firstname,' ',lastname) as fullname, email 
			from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV') ) can on can.id = a.ts2_candidate_contact_c --CANDIDATE
where (j.job_id is not null and can.id is not null) --j.id = 'a0K0J00000dNO6SUAW'

UNION ALL

       select 
            a.id as placementid, a."name" as "JobApplicationName"
            , j.company_id, j.company_name
            , j.contact_id, j.contact_fullname
            , j.job_id, j.job_number, j.job_title
            , case when rt.name is null then 301 --'PERMANENT'
                when rt.name in ('Perm') then 301 --'PERMANENT'
                when rt.name in ('Fixed Term Contract','Temp') then 302 --'CONTRACT'
                when rt.name in ('Temp-to-Perm') then 302 --'TEMPORARY_TO_PERMANENT'
                end as jobtype
            , can.id as can_id, can.fullname as "candidate_fullname"
            , a.CreatedDate
            , 'PLACED' as appstage
			, 'Placement' as sub_status
			, NULL as rejected_date
        -- select count(*) --8812 -- select distinct ts2_app_status_c --isdeleted -- select * 
        from ts2_placement_c a
        left join (select left(id,15) as id, name from recordtype) rt on rt.id = a.recordtypeid
        left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV') ) can on can.id = a.ts2_employee_c --CANDIDATE
        left join ( select job.id as "job_id", job.ts2_job_number_c as "job_number", job.name as "job_title"
                            , con.id as "contact_id", con.fullname as "contact_fullname"
                            , com.id as "company_id", com.companyname as "company_name"
                    from ts2_job_c job --JOB
                    left join ( select id, concat(firstname,' ',lastname) as fullname, email, title 
									from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = job.ts2_contact_c --CONTACT
					left JOIN ( select id, name as "companyname" from account) com ON com.id = job.ts2_account_c --COMPANY
					) j on j.job_id = a.ts2_job_c --JOB reference
       where (j.job_id is not null and can.id is not null)
)

, ja1 as (
       SELECT appid
            , job_id
            , can_id
            , jobtype
			, sub_status
            , appstage
            , CreatedDate::date
			, rejected_date
            , row_number() over(partition by job_id, can_id
                     ORDER BY CreatedDate desc, 
                            CASE appstage
                            WHEN 'PLACED' THEN 6
                            WHEN 'OFFERED' THEN 5
                            WHEN 'SECOND_INTERVIEW' THEN 4
                            WHEN 'FIRST_INTERVIEW' THEN 3
                            WHEN 'SENT' THEN 2
                            WHEN 'SHORTLISTED' THEN 1
                            END desc ) as rn
       FROM ja0
       where appstage not like 'CANDIDATE%' and appstage <> '' 
	   --and (job.jobPostingID is not null and candidate.candidateid is not null)
       )

--select [application-stage], count(*) from ja1 where rn = 1 group by [application-stage]
select appid
		, job_id
		, can_id
		, jobtype 
		, case when jobtype = 301 then 1
		when jobtype = 302 then 2
		else NULL end as position_type--position_type
		, sub_status
		, appstage
		, CreatedDate::date
		, rejected_date
		, 3 as draft_offer --used to move offered to placed in vc [offer]
		, 2 as invoicestatus --used to update invoice status in vc [invoice] as 'active'
		, 1 as renewal_index --default value in vc [invoice]
		, 1 as renewal_flow_status --used to update flow status in VC [invoice] as 'placement_active'
		, 1 as invoice_valid
		, -10 as latest_user_id
		, current_timestamp as latest_update_date
		, 0 as tax_rate
		, 'other' as export_data_to
		, 0 as net_total
		, 0 as other_invoice_items_total
		, 0 as invoice_total
from ja1
where rn = 1
--and [application-stage] = 'PLACED' --and [#Candidate Name] like '%Freeman%'
--and appstage = 'PLACED' --8461
--and sub_status is not NULL
--and rejected_date is not NULL --483 rows