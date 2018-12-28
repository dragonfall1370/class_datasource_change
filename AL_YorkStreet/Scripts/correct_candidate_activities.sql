select * from activity_candidate

select * from activity

-- delete from activity_candidate

select id, candidate_id, category, type, content from activity
where category = 'comment'
and type = 'candidate'
and position('status history' in content) >= 0
order by candidate_id