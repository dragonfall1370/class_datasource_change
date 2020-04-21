--REQUIREMENTS
/*
--29018-parmeet.shergill@ic-resources.com
-->28962-parm.shergill@ic-resources.com

Parmeet.Shergill@ic-resources.com
Parm.shergill@ic-resources.com


--28986-sean.figura@ic-resources.com
-->29164-Sean.figura@ic-supply.com
Sean.fugura@ic-supply.com
Sean.Figura@ic-resources.com


--28994-jeff.budd@ic-executive.com
-->29174-Jeff.budd@ic-resources.com
Jeff.budd@ic-executive.com
Jeff.budd@ic-resources.com
*/

--ACTIVITY
select count(*)
from activity
where user_account_id in (29018,28986,28994) --248 rows

select count(*)
from activity
where assigned_user_id in (29018,28986,28994) --2 rows

update activity
set 
user_account_id = 
	case user_account_id 
		when 29018 then 28962 --parmeet.shergill@ic-resources.com
		when 28986 then 29164 --sean.figura@ic-resources.com
		when 28994 then 29174 --jeff.budd@ic-executive.com
		end
where user_account_id in (29018,28986,28994) --248 rows

update activity
set 
assigned_user_id = 
	case user_account_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where assigned_user_id in (29018,28986,28994) --2 rows

--CANDIDATE OWNERS
select count(*)
from candidate
where candidate_owner_json ilike '%29018%'
or candidate_owner_json ilike '%28986%'
or candidate_owner_json ilike '%28994%' --5083 rows

select count(*)
from candidate
where user_account_id in (29018,28986,28994) --35 rows

update candidate
set candidate_owner_json = replace(replace(replace(candidate_owner_json,'29018','28962'),'28986','29164'),'28994','29174')
where candidate_owner_json ilike '%29018%'
or candidate_owner_json ilike '%28986%'
or candidate_owner_json ilike '%28994%' --5083 rows

update candidate
set user_account_id =
	case user_account_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where user_account_id in (29018,28986,28994) --35 rows

-->>Alternative: update candidate owner email
update candidate
set candidate_owner_json = 
	replace(
		replace(
			replace(candidate_owner_json,'parmeet.shergill@ic-resources.com','parm.shergill@ic-resources.com')
				, 'sean.figura@ic-resources.com','Sean.figura@ic-supply.com')
					,'jeff.budd@ic-executive.com','Jeff.budd@ic-resources.com')
where candidate_owner_json ilike '%28962%'
or candidate_owner_json ilike '%29164%'
or candidate_owner_json ilike '%29174%' --5278 rows


--COMPANY OWNERS
select count(*)
from company
where user_account_id in (29018,28986,28994) --3 rows

update company
set user_account_id =
	case user_account_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where user_account_id in (29018,28986,28994) --3 rows

--
select count(*)
from company
where company_owners ilike '%29018%'
or company_owners ilike '%28986%'
or company_owners ilike '%28994%'--108 rows

update company
set company_owners = replace(replace(replace(company_owners,'29018','28962'),'28986','29164'),'28994','29174')
where company_owners ilike '%29018%'
or company_owners ilike '%28986%'
or company_owners ilike '%28994%'--108 rows

--
select count(*)
from company
where company_owner_ids::text ilike '%29018%'
or company_owner_ids::text ilike '%28986%'
or company_owner_ids::text ilike '%28994%' --108

--auto updated on company_owner_ids

--CONTACT OWNERS
select count(*)
from contact
where user_account_id in (29018,28986,28994) --6 rows

update contact
set user_account_id =
	case user_account_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where user_account_id in (29018,28986,28994)	

--
select id, contact_owners, contact_owner_ids
from contact
where contact_owners ilike '%29018%'
or contact_owners ilike '%28986%'
or contact_owners ilike '%28994%'--2104 rows

update contact
set contact_owners = replace(replace(replace(contact_owners,'29018','28962'),'28986','29164'),'28994','29174')
where contact_owners ilike '%29018%'
or contact_owners ilike '%28986%'
or contact_owners ilike '%28994%'--2104 rows

--CANDIDATE GROUP
select *
from candidate_group
where owner_id in (29018,28986,28994) --1 rows

update candidate_group
set owner_id = 
	case owner_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where owner_id in (29018,28986,28994) --1 rows

--CONTACT GROUP
select *
from contact_group
where owner_id in (29018,28986,28994) --2 rows

update contact_group
set owner_id = 
	case owner_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where owner_id in (29018,28986,28994) --2 rows


--TEAM GROUP (0)

--TEAM GROUP USER (6)
select *
from team_group_user
where user_id in (29018,28986,28994) --6 rows

select *
from team_group_user
where user_id in (28962,29164,29174) --9 rows

--deactivate those users after

--JOB OWNERS
select *
from position_agency_consultant
where user_id in (29018,28986,28994) --1696 rows

select *
from position_agency_consultant
where user_id in (28962,29164,29174) --55 rows

update position_agency_consultant
set user_id = 
	case user_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where user_id in (29018,28986,28994) --1696 rows

--OFFER PERSONAL INFO (0)
select *
from offer_personal_info
where user_account_id in (29018,28986,28994)

select *
from offer_personal_info
where offer_letter_signatory_user_id in (29018,28986,28994)

--OFFER REVENUE SPLIT 
select *
from offer_revenue_split
where astute_user_id::integer in (29018,28986,28994) --0 row

select *
from offer_revenue_split
where user_id::integer in (29018,28986,28994) --62 rows

update offer_revenue_split
set user_id = 
	case user_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where user_id in (29018,28986,28994) 

--INTERVIEWS
select *
from interview
where additional_user_id in (29018,28986,28994) --0


select *
from interview
where user_account_id in (29018,28986,28994) --10 rows

update interview
set user_account_id =
	case user_account_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where user_account_id in (29018,28986,28994) 

--OFFER & OFFER_APPROVAL
select *
from offer
where latest_user_id in (29018,28986,28994) --4

update offer
set latest_user_id =
	case latest_user_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where latest_user_id in (29018,28986,28994) 

select *
from offer_approval
where user_account_id in (29018,28986,28994) --0

--JOB APPLICATION
select *
from position_candidate
where sub_status_update_user_id in (29018,28986,28994) --19 rows


select *
from position_candidate
where shortlisted_user_id in (29018,28986,28994) --47


select *
from position_candidate
where sent_user_id in (29018,28986,28994) --38

--
update position_candidate
set sub_status_update_user_id =
	case sub_status_update_user_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where sub_status_update_user_id in (29018,28986,28994) 


update position_candidate
set shortlisted_user_id =
	case shortlisted_user_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where shortlisted_user_id in (29018,28986,28994) 


update position_candidate
set sent_user_id =
	case sent_user_id 
		when 29018 then 28962
		when 28986 then 29164
		when 28994 then 29174
		end
where sent_user_id in (29018,28986,28994)