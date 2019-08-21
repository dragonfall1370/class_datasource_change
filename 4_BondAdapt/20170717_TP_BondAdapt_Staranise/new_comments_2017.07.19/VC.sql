-- COMPANY
select * from company where name like '%Ropes%'
or name like '%Madison%'
or name like '%K E%'
or name like '%Mischon%'
or name like '%ADV Partners%'
or name like 'Holman'
or name like 'Charles'
or name like 'Partners'
or name like 'Transamerica'
or name like 'Northern'
or name like 'NHI'

select id,external_id from company where external_id is not null  order by external_id desc  limit 10

select cc.*, _c.* from company c
left join _company _c on _c.name = c.name
left join company_comment cc on cc.company_id = c.id
where _c.name is not null and _c.name != ''

insert into company_comment(comment_content,comment_timestamp,insert_timestamp,user_id,company_id)
select    _c.comment as comment_content
        , _c.timestamp as comment_timestamp
        , _c.timestamp as insert_timestamp
        , cast('-10' as int) as user_id
        , c.id as company_id
        --, _c.NAME as company_name
from company c
left join _company _c on _c.name = c.name
where _c.name is not null and _c.name != ''

select * from company_comment where company_id = 13753 limit 10

-- CONTACT

select _c.email from contact c
left join _contact _c on _c.email = c.email
where _c.email is not null and _c.email != ''

insert into contact_comment(contact_id,user_id,comment_timestamp,insert_timestamp,comment_content)
select    c.id as contact_id --, c.first_name, c.last_name, c.email
        --, _c.firstname, _c.lastname
        , case when owner.id in (select id from user_account) then owner.id else cast('-10' as int) end as user_id --, _c.commentsby
        , _c.timestamp as comment_timestamp
        , _c.timestamp as insert_timestamp
        , _c.comments as comment_content
from contact c
left join _contact _c on _c.email = c.email
left join (select id,email from contact) owner on owner.email = c.email
where _c.email is not null and _c.email != ''


-- CANDIDATE
insert into position_candidate_feedback(candidate_id,user_account_id,contact_method,related_status,comment_body,feedback_timestamp,insert_timestamp)
-- with t as (select concat(ltrim(rtrim(_c.firstname)),'_',ltrim(rtrim(_c.lastname)),'@noemail.com') as email2 from _candidate _c where email is null or _c.email = '')
select c.id as candidate_id
        --, c.first_name, c.last_name, _c.firstname, _c.middlename, _c.lastname, _c.email, _c.commentsby
        , case when owner.id in (select id from user_account) then owner.id else cast('-10' as int) end as user_account_id
        --, cast('-10' as int) as user_account_id
        , cast('4' as int) as contact_method
        , cast('1' as int) as related_status
        , _c.comments as comment_body
        , _c.timestamp as feedback_timestamp
        , _c.timestamp as insert_timestamp
from _candidate _c
left join candidate c on _c.email ilike c.email
left join (select id,email from contact) owner on owner.email = c.email
--where _c.email is not null and _c.email <> ''
where _c.email like '%@noemail.com'

-- NEW CANDIDATE
SELECT distinct (case when cast(_c.email as varchar) = '' or _c.email is null then concat(ltrim(rtrim(_c.firstname)),'_',ltrim(rtrim(_c.lastname)),'@noemail.com') else _c.email end ) as email
        , _c.firstname, _c.middlename, _c.lastname, doc.doc
        , c.id
from _candidate _c
left join candidate c on c.email ilike _c.email
left join (select id,email from contact) owner on owner.email = c.email
left join (SELECT email, array_to_string(array_agg(doc), ',') AS doc FROM _candidate where email <> '' group by email) doc on doc.email = _c.email
where c.id is null

select id,external_id from candidate where external_id is not null order by external_id desc limit 10
select email from candidate where email like '%@noemail.com'
