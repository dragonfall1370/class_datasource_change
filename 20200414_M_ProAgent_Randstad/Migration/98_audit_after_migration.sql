--AUDIT AFTER MIGRATION
---Check duplicate activities
with activity_rn as (select id, candidate_id, content
, row_number() over(partition by candidate_id, content order by id desc) as rn
from activity
where candidate_id in (select id from candidate where external_id ilike 'CDT%'))

select * from activity_rn 
where rn > 1
and id > 698207-- max(id) from duplicate records


--BRANDS SETTINGS
select tgu.*
, u.email
from team_group_user tgu
left join (select id, email from user_account where deleted_timestamp is NULL) u on tgu.user_id = u.id
order by id desc

delete 
--select *
from team_group_user
where insert_timestamp >= '2020-02-21'
and user_id <> -10

delete
--select *
from team_group_company
where insert_timestamp >= '2020-02-21' --29485

delete
--select *
from team_group_candidate
where insert_timestamp >= '2020-02-21' --171404


delete
--select *
from team_group_position
where insert_timestamp >= '2020-02-21' --140385