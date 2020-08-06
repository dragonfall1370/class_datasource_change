with dup as (select [採用担当者ID], [メール]
	, row_number() over(partition by trim(' ' from lower([メール])) order by [採用担当者ID] asc) as rn --distinct email if emails exist more than once
	from csv_rec
	where [メール] like '%_@_%.__%')

, last_name as (select [採用担当者ID], trim([採用担当者]) as last_name
	, row_number() over(partition by trim(lower([採用担当者])) order by [採用担当者ID] desc) as rn
	from csv_rec
	where [採用担当者] <> '')

, allphone as (select [採用担当者ID] as con_ext_id
	, trim([連絡先 電話,FAX1]) as phone
	from csv_rec
	where [連絡先種類1] in ('電話') and nullif([連絡先 電話,FAX1], '') is not NULL
	
	UNION ALL
	select [採用担当者ID] as con_ext_id
	, trim([連絡先 電話,FAX2]) as phone
	from csv_rec
	where [連絡先種類2] in ('電話') and nullif([連絡先 電話,FAX2], '') is not NULL
	)

, con_phone as (select con_ext_id
	, string_agg(phone, ',') as con_phone
	from allphone
	where phone is not NULL
	group by con_ext_id)

, allmobile as (select [採用担当者ID] as con_ext_id
	, trim([連絡先 電話,FAX1]) as mobile
	from csv_rec
	where [連絡先種類1] in ('携帯') and nullif([連絡先 電話,FAX1], '') is not NULL
	
	UNION ALL
	select [採用担当者ID] as con_ext_id
	, trim([連絡先 電話,FAX2]) as mobile
	from csv_rec
	where [連絡先種類2] in ('携帯') and nullif([連絡先 電話,FAX2], '') is not NULL
	)

, con_mobile as (select con_ext_id
	, string_agg(mobile, ',') as con_mobile
	from allmobile
	where mobile is not NULL
	group by con_ext_id)

--MAIN SCRIPT
select c.[企業 PANO ]
	, case when not exists(select 1 from csv_recf where [PANO ] = c.[企業 PANO ]) then 'CPY9999999' --default company
		else c.[企業 PANO ] end as [contact-companyId]
	, trim(c.[採用担当者ID]) as [contact-externalId]
	, concat('【', c.[採用担当者ID], '】', [採用担当者]) as [contact-lastName]
	, c.[メール]
	, coalesce(c.[採用担当者ID] + '_' + nullif(trim(dup.[メール]),''), NULL) as [contact-email]
	, c.[フリガナ] as [contact-lastNameKana]
	, concat_ws(' ', c.[部署], c.[役職]) as [contact-jobTitle]
	, nullif(replace(cp.con_phone, ':', ''),'0') as [contact-phone]
	, c.[所在地 〒] as addr_no
	, c.[所在地 都道府県] as addr_state
	, c.[所在地詳細] as addr_full
	, c.[メモ] as skills --#CF
from csv_rec c
left join dup on dup.採用担当者ID = c.採用担当者ID
left join con_phone cp on cp.con_ext_id = c.採用担当者ID
--38449 rows