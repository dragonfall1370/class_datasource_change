--restore VC industry (from 19/06/2019)
select *
from vertical --252
where name not like '%BW%' --23 rows | 229 rows
order by name

--COMPANY INDUSTRY
select c.company_id
, v.name as industry
, c.insert_timestamp
from company_industry c
left join vertical v on c.industry_id = v.id --18807
where v.name not like '%BW%' --18828

--CONTACT INDUSTRY
select c.contact_id
, v.name as industry
, c.insert_timestamp
from contact_industry c
left join vertical v on c.industry_id = v.id --38376
where v.name not like '%BW%' --37528

--CANDIDATE INDUSTRY
select c.candidate_id
, v.name as industry
, c.insert_timestamp
from candidate_industry c
left join vertical v on c.vertical_id = v.id --283492
where v.name not like '%BW%' --276203

--POSITION INDUSTRY
select c.id as position_id
, v.name as industry
, c.insert_timestamp
from position_description c
left join vertical v on c.vertical_id = v.id
where v.name not like '%BW%'
and c.vertical_id is not NULL --99