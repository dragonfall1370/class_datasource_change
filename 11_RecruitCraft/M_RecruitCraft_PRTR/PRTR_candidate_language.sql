/* LANGUAGE JSON SAMPLE

[{"languageCode":"tl","level":""},{"languageCode":"en","level":""}]

*/

--MAIN SCRIPT
with CandidateLang as (select lp_id
	, parent_id
	, case when lp_language = 'Cantonese' then 'zh_TW'
	when lp_language = 'Dutch' then 'nl'
	when lp_language = 'English' then 'en'
	when lp_language = 'French' then 'fr'
	when lp_language = 'German' then 'de'
	when lp_language = 'Italian' then 'it'
	when lp_language = 'Japanese' then 'ja'
	when lp_language = 'Korean' then 'ko'
	when lp_language = 'Malay' then 'ms'
	when lp_language = 'Mandarin' then 'zh'
	when lp_language = 'Portuguese' then 'pt'
	when lp_language = 'Spanish' then 'es'
	when lp_language = 'Tagalog' then 'tl'
	else NULL end as CanLang
	from candidate.LanguageProficiencies
	where IsDeleted = 0)

select distinct concat('PRTR',cl.parent_id) as CandidateExtID
, (select CanLang as languageCode
	, '' as level
	from CandidateLang where parent_id = cl.parent_id
	and CanLang is not NULL
	order by lp_id desc
	for json path
	) as CandidateLanguage
from CandidateLang cl
order by CandidateExtID