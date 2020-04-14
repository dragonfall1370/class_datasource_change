--candidate phone
select [PANO ]
, 連絡先種別1
, trim(replace(連絡先1, ':', '')) as phone
from csv_can
where 連絡先種別1 = '電話'
and 連絡先1 <> ''

UNION ALL

select [PANO ]
, 連絡先種別2
, trim(replace(連絡先2, ':', '')) as phone
from csv_can
where 連絡先種別2 = '電話'
and 連絡先2 <> ''


--candidate mobile
select [PANO ]
, 連絡先種別1
, trim(replace(連絡先1, ':', '')) as phone
from csv_can
where 連絡先種別1 = '携帯'
and 連絡先1 <> ''

UNION ALL

select [PANO ]
, 連絡先種別2
, trim(replace(連絡先2, ':', '')) as phone
from csv_can
where 連絡先種別2 = '携帯'
and 連絡先2 <> ''


--candidate fax
select [PANO ]
, 連絡先種別1
, trim(replace(連絡先1, ':', '')) as phone
from csv_can
where 連絡先種別1 = 'FAX'
and 連絡先1 <> ''

UNION ALL

select [PANO ]
, 連絡先種別2
, trim(replace(連絡先2, ':', '')) as phone
from csv_can
where 連絡先種別2 = 'FAX'
and 連絡先2 <> ''

--candidate other phones
with can_phone as (select [PANO ]
	, replace(replace(replace(replace(その他連絡先, '[種別]', '|'), '[名称]', '*'), '[連絡先]', '\'), char(10), '') as alt_phone
	from csv_can
	where その他連絡先 <> '')

, alt_phone as (select [PANO ]
	, value as alt_phone
	from can_phone
	cross apply string_split(alt_phone, '|')
	where alt_phone <> '')

, phone as (select [PANO ] as cand_ext_id
	, alt_phone
	, case when charindex('*', alt_phone) > 1 then left(alt_phone, charindex('*', alt_phone) - 1)
		else NULL end as phone_type
	, case when charindex('\', alt_phone) > 1 then right(alt_phone, len(alt_phone) - charindex('\', alt_phone))
		else NULL end as phone
	from alt_phone
	where alt_phone <> '')

select *
from phone
where phone_type is not NULL and phone <> ''