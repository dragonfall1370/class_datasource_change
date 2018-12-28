select category, type, comment_type, company_id, contact_id, candidate_id from Activity
where
	user_account_id = -10
	and (company_id in (
			select id from company
			where external_id is not null
				and external_id <> '0'
		)
		or contact_id in (
			select id from contact
			where external_id is not null
				and external_id <> '0'
		)
		or candidate_id in (
			select id from candidate
			where external_id is not null
				and external_id <> '0'
		)
	)

select * from activity

select * from activity_contact

select * from activity_candidate


select * from activity where company_id is null

select * from activity_job
order by insert_timestamp desc
limit 1

select id, content from activity
where id = 9724

-- delete from activity_job
-- where activity_id = 9724 and job_id = 32470
-- 
-- delete from activity
-- where id = 9724