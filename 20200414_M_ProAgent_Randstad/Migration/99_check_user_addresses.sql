---> USER ACCOUNT
select *
from user_account
where deleted_timestamp is null
and email ilike '%akihito.otani@randstad.co.jp%' --29207 inactive | 29436 active

select *
from user_account
where deleted_timestamp is null
and email ilike '%richiko.tsukiyama@randstad.co.jp%' --29467 | 28961

---> MAKE DUMMY AND ACTIVE
--make 29436 (created on 2020-02-21 inactive and dummy)
update user_account
set email = 'dummy_' || email
where id = 29436

update user_account
set email = 'dummy_' || email
where id = 29467

---> COUNT OWNERS
select id, company_owner_id
from company
where company_owner_id in (29436, 29207)

select id, company_owner_id
from company
where company_owner_id in (29467, 28961) --28961: 60 cases
--
select id, contact_owners
from contact
where contact_owners ilike '%29436%' or contact_owners ilike '%29207%'

select id, contact_owners
from contact
where contact_owners ilike '%29467%' or contact_owners ilike '%28961%' --28961: 303 cases

--
select id, candidate_owner_json
from candidate
where candidate_owner_json ilike '%29436%' or candidate_owner_json ilike '%29207%' --29207: 1 case


select id, candidate_owner_json
from candidate
where candidate_owner_json ilike '%29467%' or candidate_owner_json ilike '%28961%' --28961: 1473 cases

--
select *
from position_agency_consultant
where user_id in (29436, 29207)

select *
from position_agency_consultant
where user_id in (29467, 28961) --28961: 815 cases

---> GROUP BY EMAIL ADDRESSES
select trim(lower(email)), count(*)
from user_account
group by trim(lower(email))
having count(*) > 1