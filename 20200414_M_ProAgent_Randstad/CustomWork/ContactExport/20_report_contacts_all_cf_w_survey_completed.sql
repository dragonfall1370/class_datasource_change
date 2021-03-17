with cf_value as (select cffv.form_id as join_form_id
	, cffv.field_id as join_field_id
	, cfl.translate as join_field_translate
	, cffv.field_value as join_field_value
	from configurable_form_language cfl
	left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
	where cfl.language = 'en' --input language
	and cffv.field_id in (11334,11344,11346,11348,11349,11350,11351,11353,11354,11355,11356,11357,11358) --input field
	order by cffv.field_id, cffv.field_value::int
	)

--CF11334
, cf_11334_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11334
	and nullif(field_value, '') is not NULL)
	
, cf_11334 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11334_value
	from cf_11334_split cf
	left join (select * from cf_value where join_field_id = 11334) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11334
	
--CF11344
, cf_11344_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11344
	and nullif(field_value, '') is not NULL)
	
, cf_11344 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11344_value
	from cf_11344_split cf
	left join (select * from cf_value where join_field_id = 11344) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11344

--CF11348
, cf_11348_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11348
	and nullif(field_value, '') is not NULL)
	
, cf_11348 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11348_value
	from cf_11348_split cf
	left join (select * from cf_value where join_field_id = 11348) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11348
	
--CF11349
, cf_11349_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11349
	and nullif(field_value, '') is not NULL)
	
, cf_11349 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11349_value
	from cf_11349_split cf
	left join (select * from cf_value where join_field_id = 11349) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11349
	
	
--CF11350
, cf_11350_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11350
	and nullif(field_value, '') is not NULL)
	
, cf_11350 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11350_value
	from cf_11350_split cf
	left join (select * from cf_value where join_field_id = 11350) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11350
	
	
--11351
, cf_11351_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11351
	and nullif(field_value, '') is not NULL)
	
, cf_11351 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11351_value
	from cf_11351_split cf
	left join (select * from cf_value where join_field_id = 11351) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11351
	
	
--11353
, cf_11353_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11353
	and nullif(field_value, '') is not NULL)
	
, cf_11353 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11353_value
	from cf_11353_split cf
	left join (select * from cf_value where join_field_id = 11353) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11353
	
	
--11357
, cf_11357_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11357
	and nullif(field_value, '') is not NULL)
	
, cf_11357 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11357_value
	from cf_11357_split cf
	left join (select * from cf_value where join_field_id = 11357) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11357
	
--11346
, cf_11346_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11346
	and nullif(field_value, '') is not NULL)
	
, cf_11346 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11346_value
	from cf_11346_split cf
	left join (select * from cf_value where join_field_id = 11346) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11346
	
	
--11354
, cf_11354_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11354
	and nullif(field_value, '') is not NULL)
	
, cf_11354 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11354_value
	from cf_11354_split cf
	left join (select * from cf_value where join_field_id = 11354) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11354
	
	
--11355
, cf_11355_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11355
	and nullif(field_value, '') is not NULL)
	
, cf_11355 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11355_value
	from cf_11355_split cf
	left join (select * from cf_value where join_field_id = 11355) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11355
		
		
--11356
, cf_11356_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11356
	and nullif(field_value, '') is not NULL)
	
, cf_11356 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11356_value
	from cf_11356_split cf
	left join (select * from cf_value where join_field_id = 11356) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11356
	
	
--11358
, cf_11358_split as (select additional_id
	, field_value
	, unnest(string_to_array(field_value, ',')) as split_field_value
	from additional_form_values
	where field_id = 11358
	and nullif(field_value, '') is not NULL)
	
, cf_11358 as (select additional_id
	, string_agg(distinct join_field_translate, ', ') as cf_11358_value
	from cf_11358_split cf
	left join (select * from cf_value where join_field_id = 11358) v on v.join_field_value = cf.split_field_value
	group by additional_id) --select * from cf_11358

--Contact FE/SFE
, vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

, contact_fe_sfe as (select cfe.contact_id
	, cfe.functional_expertise_id
	, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
	, cfe.sub_functional_expertise_id
	, v.sfe
	from contact_functional_expertise cfe
	left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
	)
	
, contact_fe_group_sfe as (select contact_id, fe
	, string_agg(sfe, '; ') as group_sfe
	from contact_fe_sfe
	where coalesce(sfe, fe) is not NULL
	group by contact_id, fe)
	
, contact_fe_group as (select contact_id
	, string_agg(fe, ' | ' order by fe desc) as group_fe
	, string_agg(group_sfe, ' | ' order by fe desc) as group_sfe
	from contact_fe_group_sfe
	group by contact_id)
	
--CONTACT INDUSTRY/SUB INDUSTRY
, contact_ind as (select ci.contact_id
	, v.parent_id as ind_id
	, v.id as sub_ind_id
	, v.name as sub_ind
	from contact_industry ci
	left join (select * from vertical where parent_id is not NULL) v on ci.industry_id = v.id --sub industry
	)
	
, contact_ind_group_sub_ind as (select ci.contact_id
	, v.name as ind
	, string_agg(sub_ind, '; ' order by v.name desc) as group_sub_ind
	from contact_ind ci
	left join (select * from vertical where parent_id is NULL) v on v.id = ci.ind_id --industry
	where coalesce(sub_ind_id, ind_id) is not NULL
	group by ci.contact_id, v.name
	)
	
, contact_ind_group as (select contact_id
	, string_agg(ind, ' | ' order by ind desc) as group_industry
	, string_agg(group_sub_ind, ' | ' order by ind desc) as group_sub_industry
	from contact_ind_group_sub_ind
	group by contact_id)
	
--BRAND
, contact_brand as (select tgc.contact_id
	, string_agg(tg.name, ', ' order by tg.name) as contact_brand
	from team_group_contact tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	group by tgc.contact_id)

--BRANCH
, contact_branch as (select b.record_id as contact_id
	, string_agg(tg.name, ', ' order by tg.name) as contact_branch
	from branch_record b
	left join (select * from team_group where group_type = 'BRANCH') tg on tg.id = b.branch_id
	where b.record_type = 'contact'
	group by b.record_id)

--CONTACT OWNERS
, contact_owners as (select id, json_array_elements_text(contact_owners::json) as owners_id
	from contact
	where contact_owners is not NULL)
	
, contact_owner_group as (select co.id
	, string_agg(name, ', ' order by u.id) as contact_owner_group
	from contact_owners co
	left join user_account u on u.id = co.owners_id::int
	group by co.id)

--CONTACT WORK LOCATION
, contact_loc as (select cl.contact_id
	, coml.location_name
	, row_number() over (partition by cl.contact_id order by coml.id) as rn
	from contact_location cl
	left join company_location coml on coml.id = cl.company_location_id
	where nullif(coml.location_name, '') is not NULL
	)
	
---MAIN SCRIPT
select c.id
, c.first_name
, c.last_name
, c.first_name_kana
, c.last_name_kana
, c.email
, c.personal_email
, c.phone
, c.mobile_phone
, c.home_phone
, com.name as company_name
, cl.location_name as work_location
, c.job_level
, c.hierarchy as job_level_hierarchy
, c.job_title
, c.linkedin
, c.skype
, c.nick_name
, cind.group_industry
, cind.group_sub_industry
, cfe.group_fe
, cfe.group_sfe
, cb.contact_brand
, cog.contact_owner_group
, cf_11276.field_value "PA Contact ID"
, cf_11334_value as "Marketing Content Shared"
, 'YES' as "Survey Completed"
, cf_11345.field_date_value "Survey Completed On"
, cf_11346_value "1. コロナ禍が貴社の事業全体にもたらした影響はどの程度だと思いますか?  -  What impact do you think the corona problem has on your overall business? (Option) Single answer"
, cf_11347.field_value "Survey Completed By (consultant email address)"
, cf_11348_value "2. コロナ禍の影響をうけ、現在、貴社が直面しているな組織上の課題は何ですか。 　　　該当するもの全てにチェックをしてください　(選択肢)　複数回答  -  What organizational challenges are you currently facing in the wake of the COVID-19 crisis?  　　　Check all applicable item"
, cf_11361.field_value "2. その他（自由回答）"

, cf_11350_value "3. コロナ禍により生じた財政的な課題を解決すために、貴社では、どのような施策を実施または検討されていますか。あてはまる内容を全て選択してください。  -  What measures are implemented or considered by your company in order to solve the financial problems caused by COVID-19?"
, cf_11362.field_value "3. その他（自由回答）"
, cf_11353_value "4. 新型コロナウイルス感染拡大をうけ発令された非常事態宣言の期間中、貴社がとった人事的施策として、あてはまるものを全て選択してください。  -  During the period of state of emergency in response to the spread of COVID-19, choose all applicable personnel measures taken."
, cf_11363.field_value "4. その他（自由回答）"
, cf_11357_value "5. 様々な制限が緩和され、ニューノーマルに基づく日常が取り戻されつつある現在の、貴社の人事施策として、全てお答えください。 With the relaxing of restrictions and returning to life based on the new normal, identify all personnel measures being considered by your"
, cf_11364.field_value "5. その他（自由回答）"
, cf_11354_value "６．今後の見通しについてお尋ねします。 　　　コロナ禍からの回復を経て、貴社の採用が積極局面を迎える時期はいつ頃とお考えですか。   -  What are the outlook for the future？  　　　When do you think a positive phase for your company to hiring will begin after COVID-19?"
, cf_11355_value "６－２．採用等により、人員の補充が必要(見込み)となる部署数はどの程度ですか  -  How many departments do you expect personnel to be added due to hiring?"
, cf_11356_value "６－３．採用等により、人員の補充が必要(見込み)となる職種の数はどの程度ですか。  -  How many profiles do you expect to be replenished due to hiring, etc.?"
, cf_11358_value "６－４　採用または補充予定の人数総数はどの程度ですか 。  -  How many people do you plan to hire or replenish?"
, cf_11351_value "7. 回復・成長期の人材活用において、課題としてどんなことが考えられますか。可能性のあるものを全てお答えください 。  -  What do you think is a challenge in the utilization of human resources during the recovery and growth phase? Please answer all possible"
, cf_11352.field_value "7.  その他　（自由回答）"
, cf_11349_value "8. 現在の課題を解決するサポートとして、ランスタッドへ期待することは何ですか。あてはまるものを全て選択してください。 （選択肢）　複数回答  -  What do you expect from Randstad to help you solve the current challenge?  Select all applicable items."
, cf_11360.field_value "8.  その他　（自由回答）"
from contact c
left join (select * from additional_form_values where field_id = 11276) cf_11276 on cf_11276.additional_id = c.id
left join (select * from additional_form_values where field_id = 11345) cf_11345 on cf_11345.additional_id = c.id
left join (select * from additional_form_values where field_id = 11347) cf_11347 on cf_11347.additional_id = c.id
left join (select * from additional_form_values where field_id = 11352) cf_11352 on cf_11352.additional_id = c.id
left join (select * from additional_form_values where field_id = 11360) cf_11360 on cf_11360.additional_id = c.id
left join (select * from additional_form_values where field_id = 11361) cf_11361 on cf_11361.additional_id = c.id
left join (select * from additional_form_values where field_id = 11362) cf_11362 on cf_11362.additional_id = c.id
left join (select * from additional_form_values where field_id = 11363) cf_11363 on cf_11363.additional_id = c.id
left join (select * from additional_form_values where field_id = 11364) cf_11364 on cf_11364.additional_id = c.id
left join cf_11334 on cf_11334.additional_id = c.id
left join cf_11346 on cf_11346.additional_id = c.id
left join cf_11348 on cf_11348.additional_id = c.id
left join cf_11349 on cf_11349.additional_id = c.id
left join cf_11350 on cf_11350.additional_id = c.id
left join cf_11351 on cf_11351.additional_id = c.id
left join cf_11353 on cf_11353.additional_id = c.id
left join cf_11354 on cf_11354.additional_id = c.id
left join cf_11355 on cf_11355.additional_id = c.id
left join cf_11356 on cf_11356.additional_id = c.id
left join cf_11357 on cf_11357.additional_id = c.id
left join cf_11358 on cf_11358.additional_id = c.id
left join contact_fe_group cfe on cfe.contact_id = c.id
left join contact_ind_group cind on cind.contact_id = c.id
left join contact_brand cb on cb.contact_id = c.id
left join company com on com.id = c.company_id
left join contact_owner_group cog on cog.id = c.id
left join (select * from contact_loc where rn=1) cl on cl.contact_id = c.id
join (select * from additional_form_values where field_id = 11344 and field_value = '1') cf_11344 on cf_11344.additional_id = c.id --filter with survey completed
where c.deleted_timestamp is NULL
--and c.id = 28566
order by c.id