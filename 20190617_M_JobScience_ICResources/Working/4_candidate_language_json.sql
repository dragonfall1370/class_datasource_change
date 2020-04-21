--CANDIDATE LANGUAGE JSON
--CONTACT/ CANDIDATE 'Hires Contractor (IC)' as Industry
with candidatelang as (select ts2_contact_c as candidate_id
		, ts2_skill_name_c
		, case lower(ts2_skill_name_c)
				when lower('Chinese (IC)') then 'zh_CN'
				when lower('Czech (IC)') then 'cs'
				when lower('Dutch (IC)') then 'nl'
				when lower('French (IC)') then 'fr'
				when lower('German (IC)') then 'de'
				when lower('Italian (IC)') then 'it'
				when lower('Japanese (IC)') then 'ja'
				when lower('Korean (IC)') then 'ko'
				when lower('Malaysian (IC)') then 'ml'
				when lower('Portuguese (IC)') then 'pt'
				when lower('Spanish (IC)') then 'es'
				when lower('Swedish (IC)') then 'sv'
				when lower('English (IC)') then 'en'
				else NULL end as candlanguage
		, '' as level
		, now() as insert_timestamp
		from ts2_skill_c
		where lower(ts2_skill_name_c) in (lower('Chinese (IC)'), lower('Czech (IC)'), lower('Dutch (IC)'), lower('French (IC)'), lower('German (IC)')
																, lower('Italian (IC)'), lower('Japanese (IC)'), lower('Korean (IC)'), lower('Malaysian (IC)'), lower('Portuguese (IC)')
																, lower('Spanish (IC)'), lower('Swedish (IC)'), lower('English (IC)'))
)

/*
select candidate_id, count(*)
from candidatelang
group by candidate_id
having count(*) > 1 --0030Y00000j0tD7QAI

select *
from candidatelang
where candidate_id = '0030Y00000j0tD7QAI'
*/

select distinct candidate_id
, (select array_to_json(array_agg(row_to_json(value_json)))
					from (
							select candlanguage as "languageCode" --keep format by using ""
							, '' as "level"
							from candidatelang cl
							where c.candidate_id = cl.candidate_id
							) as value_json )::text as field_value_json
from candidatelang c
--where c.candidate_id = '0030Y00000j0tD7QAI'