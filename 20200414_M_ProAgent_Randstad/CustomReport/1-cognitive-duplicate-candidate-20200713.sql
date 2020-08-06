with dup_cognitive as (select additional_id
	from additional_form_values
	where field_id = 11302
	and field_value = '66' -- Duplicate
	)

, language_split as (select id
	, jsonb_array_elements(skill_details_json::jsonb)->>'languageCode' as language_split
	from candidate
	where id in (select additional_id from dup_cognitive)) --select * from language_split where id = 45308
	
, language_group as (select ls.id
	, string_agg(distinct l.system_name, ', ') as candidate_lang
	from language_split ls
	left join language l on l.code = ls.language_split
	where 1=1
	--and ls.id = 45308
	group by ls.id)
	
, owner_split as (select id
	, candidate_owner_json
	, jsonb_array_elements(candidate_owner_json::jsonb)->>'ownerId' as owner_id
	from candidate
	where id in (select additional_id from dup_cognitive)) --select * from owner_split
	
, owner_group as (select o.id
	, string_agg(distinct u.name, ', ') as candidate_owners
	from owner_split o
	left join user_account u on u.id = o.owner_id::int
	group by o.id)

select c.id as "Candidate ID"
, o.candidate_owners as "候補者担当者"
, case c.active
		when 1 then 'active' 
		when 0 then 'passive'
		when 2 then 'do not contact'
		end as "ステータス" --active_status
, cs.name as "登録経路"
, c.insert_timestamp as "登録日"
--, c.external_id
, u.name as "登録時の担当者"
, c.note as "ノート"
, c.first_name || ' ' || c.last_name as "名"
, c.first_name_kana as "kana/ローマ字(名)"
, c.middle_name as "ミドルネーム"
, c.last_name_kana as "kana/ローマ字（姓）"
, c.date_of_birth "生年月日"
, lg.candidate_lang "希望する言語"
, c.email as "Eメール"
, c.phone as "電話番号" --phone
, c.phone2 as "携帯電話" --mobile
, '' as "優先"
, c.home_phone as "自宅電話"
, '' as "優先"
, c.work_phone as "勤務先電話番号"
, cl.address as "現住所"
, cl2.address as "住所2"
, cl.state as "都道府県" --prefecture
, c.current_salary as "現在年収"
, c.desire_salary as "希望年収"
, case c.working_state 
		when 1 then '現職中'
		when 2 then '離職'
		when 3 then '就学中'
		end as "現職ですか？" --working_state
/* Currently working? > working_state
1 Current employed
2 Not employed
3 At school */
, c.company_count as "経験社数" --number of companies
, c.current_employer as "会社-1"
from candidate c
left join owner_group o on o.id = c.id
left join candidate_source cs on cs.id = c.candidate_source_id
left join user_account u on u.id = c.user_account_id
left join language_group lg on lg.id = c.id
left join common_location cl on cl.id = c.current_location_id
left join common_location cl2 on cl2.id = c.personal_location_id
left join (select * from candidate_work_history where index=1) cw on cw.candidate_id = c.id
where c.id in (select additional_id from dup_cognitive)
order by c.id