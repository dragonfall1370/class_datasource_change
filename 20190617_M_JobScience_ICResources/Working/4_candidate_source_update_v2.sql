-- CANDIDATE SOURCES
select
	c.id as candidate_id
	, firstname
	, lastname
	, email
	, c.recordtypeid
	, trim(c.ts2_source_c) as ts2_source_c
from contact c
where c.id in ('0030J00002GrcD4QAJ', '0030J00002GtS9ZQAV', '0030Y00000zB1McQAK')
and c.ts2_source_c is NULL --13164 | 13143

select c.id
, c.ts2_candidate_source_c
, source
, c.id as candidate_id
, firstname
, lastname
, email
, c.recordtypeid
, s.name
, trim(c.ts2_source_c) as ts2_source_c
from contact c
left join ts2_source_c s on s.ts2_source_c_1_id = c.ts2_candidate_source_c
where c.id in ('0030J00002GrcD4QAJ', '0030J00002GtS9ZQAV', '0030Y00000zB1McQAK')

--COMPARISON
select id, ts2_source_c
from contact
where recordtypeid IN ('0120Y0000013O5c','0120Y000000RZZV')
and ts2_source_c is not NULL --92355 rows

select id, ts2_candidate_source_c
from contact
where recordtypeid IN ('0120Y0000013O5c','0120Y000000RZZV')
and ts2_candidate_source_c is not NULL --7157 rows