with language_list as (select uniqueid, "128 languages codegroup  12", "129 lang lev codegroup  28"
	, a.language_code
	, a.languagern
	--, b.language_lv
	--, b.lvrn
	from f01
		, unnest(string_to_array("128 languages codegroup  12", '~')) with ordinality as a (language_code, languagern)
		--, unnest(string_to_array("129 lang lev codegroup  28", '~')) with ordinality as b (language_lv, lvrn)
	)
	
, cand_lang as (select --distinct c12.description
	uniqueid
	, languagern
	, case c12.description
		when 'Arabic' then 'ar'
		when 'Armenian' then 'hy'
		when 'Bosnian' then 'bs'
		when 'Bulgarian' then 'bg'
		when 'Cantonese' then 'zh_TW'
		when 'Catalan' then 'ca'
		when 'Chinese' then 'zh_CN'
		when 'Czech' then 'cs'
		when 'Danish' then 'da'
		when 'Dutch' then 'nl'
		when 'English' then 'en'
		when 'Estonian' then 'et'
		when 'Finnish' then 'fi'
		when 'French' then 'fr'
		when 'German' then 'de'
		when 'Greek' then 'el'
		when 'Hebrew' then 'iw'
		when 'Hindu' then 'hi'
		when 'Hungarian' then 'hu'
		when 'Indonesian' then 'in'
		when 'Irish' then 'ga'
		when 'Italian' then 'it'
		when 'Japanese' then 'ja'
		when 'Kannada' then 'kn'
		when 'Korean' then 'ko'
		when 'Latin' then 'la'
		when 'Latvian' then 'lv'
		when 'Lithuanian' then 'lt'
		when 'Macedonian' then 'mk'
		when 'Maltese' then 'mt'
		when 'Maranthi' then 'mr'
		when 'Norwegian' then 'no'
		when 'Polish' then 'pl'
		when 'Portuguese' then 'pt'
		when 'Punjabi' then 'pa'
		when 'Romanian' then 'ro'
		when 'Russian' then 'ru'
		when 'Serbian' then 'sr'
		when 'Slovakian' then 'sk'
		when 'Slovenian' then 'sl'
		when 'Somali' then 'so'
		when 'Spanish' then 'es'
		when 'Swedish' then 'sv'
		when 'Tagaleg' then 'tl'
		when 'Tamil' then 'ta'
		when 'Telugu' then 'te'
		when 'Turkish' then 'tr'
		when 'Ukranian' then 'uk'
		when 'Urdu' then 'ur'
		else NULL end as cand_lang
	--, c28.description lv_description
	--, lvrn
	from language_list l
	left join (select * from codes where codegroup = '12') c12 on c12.code = l.language_code
	--left join (select * from codes where codegroup = '28') c28 on c28.code = l.language_lv
	)


SELECT uniqueid as cand_ext_id
, json_agg(row_to_json((
       SELECT ColumnName FROM ( SELECT cand_lang) AS ColumnName ("languageCode")
			 )) order by languagern asc) AS language_json
FROM cand_lang
where cand_lang is not NULL
--where uniqueid = '' --checking
GROUP BY uniqueid