--Parse documents content as Candidate Brief tab
--Change txt to rtf before execution
with person_doc as (select uniqueid
--, replace(right("relative document path", position(E'\\' in reverse("relative document path")) - 1), 'txt', 'rtf') as cand_doc
, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) as cand_doc
, description
, to_date(date, 'DD/MM/YY') created
, row_number() over(partition by uniqueid, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) order by to_date(date, 'DD/MM/YY') desc) as rn
from f01docs1_edited)

select uniqueid cand_ext_id
, cand_doc
, description as note
, created
, case when rn = 1 then 1 else 0 end as primary_document
from person_doc
where 1=1
--and description = 'People Notes' --previous version
and description in ('Window 1', 'People Notes') --updated 20201229


--GET LIST OF CSV
select candidate_document
from candidate_document --temp for candidate document mapping
where note in ('Window 1', 'People Notes')


--STORE IN DB (SQL)
USE misco_parsed_documents
GO
create table document_prod
(id int identity(1,1)
, doc_name varchar(max)
, doc_content varbinary(max)
)

--INSERT TO POSGRES DB
create table parsed_document 
(id int
, doc_name character varying (100)
, doc_content bytea
, parsed_doc_content text
)

--CONVERT TO TEXT (POSTGRESQL)
select *
, encode(doc_content, 'escape') as parsed_doc_content
from parsed_document


--CONVERT TO TEXT (MSSQL)
select id
, doc_name
, doc_content
, convert(varchar(max), doc_content) as parsed_doc_content
from document


--INSERT TO NEW BRIEF TAB
with person_doc as (select uniqueid
	--, replace(right("relative document path", position(E'\\' in reverse("relative document path")) - 1), 'txt', 'rtf') as cand_doc
	, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) as cand_doc
	, description
	, to_date(date, 'DD/MM/YY') created
	, row_number() over(partition by uniqueid, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) order by to_date(date, 'DD/MM/YY') desc) as rn
	from f01docs1_edited
	where right("relative document path", position(E'\\' in reverse("relative document path")) - 1) <> 'rtf' --updated valid format on 20210226
)

, cand_doc as (select uniqueid cand_ext_id
	, cand_doc
	, description as note
	, created
	, replace(replace(encode(doc_content, 'escape'), chr(10), '<br/>'), chr(13), '<br/>') as parsed_doc_content --remember to switch to HTML format
	from person_doc p
	left join parsed_document pd on pd.doc_name = p.cand_doc
	where description in ('People Notes', 'Window 1')
	) --select * from cand_doc where cand_ext_id = '8081010180808280'

select cand_ext_id
, 'People Notes' title
, string_agg(concat_ws('<br/>'
			, case when note = 'People Notes' then '[People Notes]<br/>'
					when note = 'Window 1' then '[Window 1]<br/>'
					end
			, '<br/>[Created date: ' || to_char(created, 'DD/MM/YY') || ']'
			, '<br/>' || nullif(replace(replace(parsed_doc_content, chr(10), '<br/>'), chr(13), '<br/>'), '')
			), '<br/>' order by created desc) as cand_brief_tab
, current_timestamp as insert_timestamp
from cand_doc
where nullif(parsed_doc_content, '') is not NULL
group by cand_ext_id