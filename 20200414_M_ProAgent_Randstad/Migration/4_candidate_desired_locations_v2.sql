--#CF | Desired Work Location
with desired_loc as (select [PANO ] as cand_ext_id
	, trim(value) as desired_loc
	from csv_can
	cross apply string_split(勤務地, char(10))
	where coalesce(nullif(勤務地,''), NULL) is not NULL)

select cand_ext_id
, 'add_cand_info' as additional_type
, 1139 as form_id
, 11265 as field_id
, current_timestamp as insert_timestamp
--, desired_loc as location_name
--, desired_loc as location_state
, case desired_loc
	when '北海道' then concat(left('HOKKAIDO', 1), lower(right('HOKKAIDO', len('HOKKAIDO') - 1)), '  ', '北海道')
	when '青森県' then concat(left('AOMORI', 1), lower(right('AOMORI', len('AOMORI') - 1)), '  ', '青森県')
	when '岩手県' then concat(left('IWATE', 1), lower(right('IWATE', len('IWATE') - 1)), '  ', '岩手県')
	when '宮城県' then concat(left('MIYAGI', 1), lower(right('MIYAGI', len('MIYAGI') - 1)), '  ', '宮城県')
	when '秋田県' then concat(left('AKITA', 1), lower(right('AKITA', len('AKITA') - 1)), '  ', '秋田県')
	when '山形県' then concat(left('YAMAGATA', 1), lower(right('YAMAGATA', len('YAMAGATA') - 1)), '  ', '山形県')
	when '福島県' then concat(left('FUKUSHIMA', 1), lower(right('FUKUSHIMA', len('FUKUSHIMA') - 1)), '  ', '福島県')
	when '茨城県' then concat(left('IBARAKI', 1), lower(right('IBARAKI', len('IBARAKI') - 1)), '  ', '茨城県')
	when '栃木県' then concat(left('TOCHIGI', 1), lower(right('TOCHIGI', len('TOCHIGI') - 1)), '  ', '栃木県')
	when '群馬県' then concat(left('GUNMA', 1), lower(right('GUNMA', len('GUNMA') - 1)), '  ', '群馬県')
	when '埼玉県' then concat(left('SAITAMA', 1), lower(right('SAITAMA', len('SAITAMA') - 1)), '  ', '埼玉県')
	when '千葉県' then concat(left('CHIBA', 1), lower(right('CHIBA', len('CHIBA') - 1)), '  ', '千葉県')
	when '東京都' then concat(left('TOKYO-23 wards', 1), lower(right('TOKYO-23 wards', len('TOKYO-23 wards') - 1)), '  ', '東京都')
	when '神奈川県' then concat(left('KANAGAWA', 1), lower(right('KANAGAWA', len('KANAGAWA') - 1)), '  ', '神奈川県')
	when '新潟県' then concat(left('NIIGATA', 1), lower(right('NIIGATA', len('NIIGATA') - 1)), '  ', '新潟県')
	when '富山県' then concat(left('TOYAMA', 1), lower(right('TOYAMA', len('TOYAMA') - 1)), '  ', '富山県')
	when '石川県' then concat(left('ISHIKAWA', 1), lower(right('ISHIKAWA', len('ISHIKAWA') - 1)), '  ', '石川県')
	when '福井県' then concat(left('FUKUI', 1), lower(right('FUKUI', len('FUKUI') - 1)), '  ', '福井県')
	when '山梨県' then concat(left('YAMANASHI', 1), lower(right('YAMANASHI', len('YAMANASHI') - 1)), '  ', '山梨県')
	when '長野県' then concat(left('NAGANO', 1), lower(right('NAGANO', len('NAGANO') - 1)), '  ', '長野県')
	when '岐阜県' then concat(left('GIFU', 1), lower(right('GIFU', len('GIFU') - 1)), '  ', '岐阜県')
	when '静岡県' then concat(left('SHIZUOKA', 1), lower(right('SHIZUOKA', len('SHIZUOKA') - 1)), '  ', '静岡県')
	when '愛知県' then concat(left('AICHI', 1), lower(right('AICHI', len('AICHI') - 1)), '  ', '愛知県')
	when '三重県' then concat(left('MIE', 1), lower(right('MIE', len('MIE') - 1)), '  ', '三重県')
	when '滋賀県' then concat(left('SHIGA', 1), lower(right('SHIGA', len('SHIGA') - 1)), '  ', '滋賀県')
	when '京都府' then concat(left('KYOTO', 1), lower(right('KYOTO', len('KYOTO') - 1)), '  ', '京都府')
	when '大阪府' then concat(left('OSAKA', 1), lower(right('OSAKA', len('OSAKA') - 1)), '  ', '大阪府')
	when '兵庫県' then concat(left('HYOGO', 1), lower(right('HYOGO', len('HYOGO') - 1)), '  ', '兵庫県')
	when '奈良県' then concat(left('NARA', 1), lower(right('NARA', len('NARA') - 1)), '  ', '奈良県')
	when '和歌山県' then concat(left('WAKAYAMA', 1), lower(right('WAKAYAMA', len('WAKAYAMA') - 1)), '  ', '和歌山県')
	when '鳥取県' then concat(left('TOTTORI', 1), lower(right('TOTTORI', len('TOTTORI') - 1)), '  ', '鳥取県')
	when '島根県' then concat(left('SHIMANE', 1), lower(right('SHIMANE', len('SHIMANE') - 1)), '  ', '島根県')
	when '岡山県' then concat(left('OKAYAMA', 1), lower(right('OKAYAMA', len('OKAYAMA') - 1)), '  ', '岡山県')
	when '広島県' then concat(left('HIROSHIMA', 1), lower(right('HIROSHIMA', len('HIROSHIMA') - 1)), '  ', '広島県')
	when '山口県' then concat(left('YAMAGUCHI', 1), lower(right('YAMAGUCHI', len('YAMAGUCHI') - 1)), '  ', '山口県')
	when '徳島県' then concat(left('TOKUSHIMA', 1), lower(right('TOKUSHIMA', len('TOKUSHIMA') - 1)), '  ', '徳島県')
	when '香川県' then concat(left('KAGAWA', 1), lower(right('KAGAWA', len('KAGAWA') - 1)), '  ', '香川県')
	when '愛媛県' then concat(left('EHIME', 1), lower(right('EHIME', len('EHIME') - 1)), '  ', '愛媛県')
	when '高知県' then concat(left('KOCHI', 1), lower(right('KOCHI', len('KOCHI') - 1)), '  ', '高知県')
	when '福岡県' then concat(left('FUKUOKA', 1), lower(right('FUKUOKA', len('FUKUOKA') - 1)), '  ', '福岡県')
	when '佐賀県' then concat(left('SAGA', 1), lower(right('SAGA', len('SAGA') - 1)), '  ', '佐賀県')
	when '長崎県' then concat(left('NAGASAKI', 1), lower(right('NAGASAKI', len('NAGASAKI') - 1)), '  ', '長崎県')
	when '熊本県' then concat(left('KUMAMOTO', 1), lower(right('KUMAMOTO', len('KUMAMOTO') - 1)), '  ', '熊本県')
	when '大分県' then concat(left('OITA', 1), lower(right('OITA', len('OITA') - 1)), '  ', '大分県')
	when '宮崎県' then concat(left('MIYAZAKI', 1), lower(right('MIYAZAKI', len('MIYAZAKI') - 1)), '  ', '宮崎県')
	when '鹿児島県' then concat(left('KAGOSHIMA', 1), lower(right('KAGOSHIMA', len('KAGOSHIMA') - 1)), '  ', '鹿児島県')
	when '沖縄県' then concat(left('OKINAWA', 1), lower(right('OKINAWA', len('OKINAWA') - 1)), '  ', '沖縄県')
	when '海外' then concat(left('OVERSEAS', 1), lower(right('OVERSEAS', len('OVERSEAS') - 1)), '  ', '海外')
	else NULL end as original_mapping
, case desired_loc
	when '北海道' then 'Hokkaido  北海道'
	when '青森県' then 'Aomori  青森'
	when '岩手県' then 'Iwate  岩手'
	when '宮城県' then 'Miyagi  宮城'
	when '秋田県' then 'Akita  秋田'
	when '山形県' then 'Yamagata  山形'
	when '福島県' then 'Fukushima  福島'
	when '茨城県' then 'Ibaraki  茨城'
	when '栃木県' then 'Tochigi  栃木'
	when '群馬県' then 'Gunma  群馬'
	when '埼玉県' then 'Saitama  埼玉'
	when '千葉県' then 'Chiba  千葉'
	when '東京都' then 'Tokyo-23 wards'
	when '神奈川県' then 'Kanagawa  神奈川'
	when '新潟県' then 'Niigata  新潟'
	when '富山県' then 'Toyama  富山'
	when '石川県' then 'Ishikawa  石川'
	when '福井県' then 'Fukui  福井'
	when '山梨県' then 'Yamanashi  山梨'
	when '長野県' then 'Nagano  長野'
	when '岐阜県' then 'Gifu  岐阜'
	when '静岡県' then 'Shizuoka  静岡'
	when '愛知県' then 'Aichi  愛知'
	when '三重県' then 'Mie  三重'
	when '滋賀県' then 'Shiga  滋賀'
	when '京都府' then 'Kyoto  京都'
	when '大阪府' then 'Osaka  大阪'
	when '兵庫県' then 'Hyogo  兵庫'
	when '奈良県' then 'Nara  奈良'
	when '和歌山県' then 'Wakayama  和歌山'
	when '鳥取県' then 'Tottori  鳥取'
	when '島根県' then 'Shimane  島根'
	when '岡山県' then 'Okayama  岡山'
	when '広島県' then 'Hiroshima  広島'
	when '山口県' then 'Yamaguchi  山口'
	when '徳島県' then 'Tokushima  徳島'
	when '香川県' then 'Kagawa  香川'
	when '愛媛県' then 'Ehime  愛媛'
	when '高知県' then 'Kochi  高知'
	when '福岡県' then 'Fukuoka  福岡'
	when '佐賀県' then 'Saga  佐賀'
	when '長崎県' then 'Nagasaki  長崎'
	when '熊本県' then 'Kumamoto  熊本'
	when '大分県' then 'Oita  大分'
	when '宮崎県' then 'Miyazaki  宮崎'
	when '鹿児島県' then 'Kagoshima  鹿児島'
	when '沖縄県' then 'Okinawa  沖縄'
	when '海外' then 'Overseas  海外'
	else NULL end as field_value
from desired_loc