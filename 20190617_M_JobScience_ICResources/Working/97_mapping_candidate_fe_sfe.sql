--CREATE MAPPING CANDIDATE FE SFE
create table candidate_fe_sfe
(id integer
,js_category character varying(1000)
,sector character varying(1000)
,required character varying(1000)
,vcfe character varying(1000)
,vcsfe character varying(1000)
,comment character varying(1000)
,note character varying(1000)
)

--CLEAN UP FE/SFE
select * from functional_expertise --19

select * from sub_functional_expertise --183

select * from contact_functional_expertise --24

select * from candidate_functional_expertise --40

select * from position_description_functional_expertise --21


--FE/SFE settings
--FE
select distinct vcfe
, CURRENT_TIMESTAMP as insert_timestamp
from candidate_fe_sfe
where note = 'Candidate category'
order by vcfe --11

--FE SFE
select distinct vcfe
, vcsfe
, CURRENT_TIMESTAMP as insert_timestamp
from candidate_fe_sfe
where note = 'Candidate category'
order by vcfe, vcsfe --331