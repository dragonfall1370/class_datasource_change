select * from bulk_upload

-- UPDATE BULK_UPLOAD SET STATUS = 'COMPLETE'
-- WHERE ID = 96

select count(*) from position_description
where deleted_timestamp is null
and name like 'Floated Candidate - %'

select * from position_description
where deleted_timestamp is null
and name like 'Floated Candidate - %'
limit 10


select id, external_id, name, floated_job from position_description
where deleted_timestamp is null
and id = 35004

-- update position_description
-- set floated_job = 0
-- where deleted_timestamp is null
-- and id = 35004

-- update position_description
-- set floated_job = 1
-- where deleted_timestamp is null
-- and name like 'Floated Candidate - %'

select count(*) from position_candidate
--where deleted_timestamp is null

select * from position_description
where id in (
select position_description_id from position_candidate
)

select count(*) from activity
select * from activity