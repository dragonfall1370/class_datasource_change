--Internal description
select [PANO ] as job_ext_id
, replace(coalesce('<p>' + 
	nullif(concat_ws('<br/>'
		, coalesce('<br/>' + '<strong>【社内データメモ】</strong>' + '<br/>' + nullif([社内データメモ], ''), NULL) --internal data memo
		, coalesce('<br/>' + '<strong>【【社内用項目】　（複数選択可）】</strong>' + '<br/>' + nullif([【社内用項目】　（複数選択可）], ''), NULL) --internal items
		, coalesce('<br/>' + '<strong>【月額 下限】</strong>' + '<br/>' + nullif([月額 下限], ''), NULL) --monthly amount from
		, coalesce('<br/>' + '<strong>【月額 上限】</strong>' + '<br/>' + nullif([月額 上限], ''), NULL) --monthly amount to
		, coalesce('<br/>' + '<strong>【時給 下限】</strong>' + '<br/>' + nullif([時給 下限], ''), NULL) --hour salary from
		, coalesce('<br/>' + '<strong>【時給 上限】</strong>' + '<br/>' + nullif([時給 上限], ''), NULL) --hour salary to
		, coalesce('<br/>' + '<strong>【求める人物像】</strong>' + '<br/>' + nullif([求める人物像], ''), NULL) --Desired image of a person
		, coalesce('<br/>' + '<strong>【ココがおすすめ！】</strong>' + '<br/>' + nullif([ココがおすすめ！], ''), NULL) --Here is recommended
		, coalesce('<br/>' + '<strong>【企業が候補者に一番期待すること】</strong>' + '<br/>' + nullif([企業が候補者に一番期待すること], ''), NULL) --What companies most expect from candidates
		, coalesce('<br/>' + '<strong>【その他メモ】</strong>' + '<br/>' + nullif(その他メモ, ''), NULL) --Other notes
		), '') + '</p>', NULL)
		, char(10), '<br/>') as internal_description
from csv_job
where coalesce(nullif([社内データメモ], ''), nullif([【社内用項目】　（複数選択可）], '')
	, nullif([求める人物像], ''), nullif([ココがおすすめ！], ''), nullif([企業が候補者に一番期待すること], ''), nullif([その他メモ], '')) is not NULL
	

--Public description (NEW - 20200219)
select [PANO ] as job_ext_id
, coalesce('<p>' + 
	nullif(coalesce('<strong>【業務内容】</strong>' + '<br/>' + nullif(replace([業務内容], char(10), '<br/>'), ''), NULL) --business content
			, '') + '</p>', NULL) as public_description
from csv_job
where nullif([業務内容], '') is not NULL

/*
--Public description (OLD VERSION)
select [PANO ] as job_ext_id
, coalesce('<p>' + 
	nullif(concat_ws(concat(char(10),char(13))
		, coalesce('【業務内容】' + char(10) + nullif([業務内容], ''), NULL) --business content
		, coalesce('【学歴】' + char(10) + nullif([学歴], ''), NULL) --removed 20200103
		), '') + '</p>', NULL) as public_description
from csv_job
where coalesce(nullif([業務内容], ''), nullif([学歴], '')) is not NULL
*/