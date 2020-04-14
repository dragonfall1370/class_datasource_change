--#CF | Prefecture | Multiple selection
with job_loc as (select distinct [PANO ] as job_ext_id
	--, [企業 PANO ] as com_ext_id
	--, value as job_work_location
	--, value as work_location_name
	, value as prefecture
from csv_job
	cross apply string_split(勤務地, char(10))
where 1=1
and 勤務地 <> ''
--and [PANO ] = 'JOB011870'
)

select job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1113 as field_id
, case prefecture
	when '北海道' then 'HOKKAIDO'
	when '青森県' then 'AOMORI'
	when '岩手県' then 'IWATE'
	when '宮城県' then 'MIYAGI'
	when '秋田県' then 'AKITA'
	when '山形県' then 'YAMAGATA'
	when '福島県' then 'FUKUSHIMA'
	when '茨城県' then 'IBARAKI'
	when '栃木県' then 'TOCHIGI'
	when '群馬県' then 'GUNMA'
	when '埼玉県' then 'SAITAMA'
	when '千葉県' then 'CHIBA'
	when '東京都' then 'TOKYO-23 wards'
	when '神奈川県' then 'KANAGAWA'
	when '新潟県' then 'NIIGATA'
	when '富山県' then 'TOYAMA'
	when '石川県' then 'ISHIKAWA'
	when '福井県' then 'FUKUI'
	when '山梨県' then 'YAMANASHI'
	when '長野県' then 'NAGANO'
	when '岐阜県' then 'GIFU'
	when '静岡県' then 'SHIZUOKA'
	when '愛知県' then 'AICHI'
	when '三重県' then 'MIE'
	when '滋賀県' then 'SHIGA'
	when '京都府' then 'KYOTO'
	when '大阪府' then 'OSAKA'
	when '兵庫県' then 'HYOGO'
	when '奈良県' then 'NARA'
	when '和歌山県' then 'WAKAYAMA'
	when '鳥取県' then 'TOTTORI'
	when '島根県' then 'SHIMANE'
	when '岡山県' then 'OKAYAMA'
	when '広島県' then 'HIROSHIMA'
	when '山口県' then 'YAMAGUCHI'
	when '徳島県' then 'TOKUSHIMA'
	when '香川県' then 'KAGAWA'
	when '愛媛県' then 'EHIME'
	when '高知県' then 'KOCHI'
	when '福岡県' then 'FUKUOKA'
	when '佐賀県' then 'SAGA'
	when '長崎県' then 'NAGASAKI'
	when '熊本県' then 'KUMAMOTO'
	when '大分県' then 'OITA'
	when '宮崎県' then 'MIYAZAKI'
	when '鹿児島県' then 'KAGOSHIMA'
	when '沖縄県' then 'OKINAWA'
	when '海外' then 'OVERSEAS'
else NULL end as field_value
from job_loc
where nullif(prefecture, '') is not NULL