
SELECT top 300
         CA.candidateID as additional_id  
        , 'add_cand_info' as additional_type
        , 1005 as form_id
        , 1016 as field_id
        , cf.field_value, case when CA.customText5 = 'Non White' then 'Non-White' else CA.customText5 end
        --, cf.field_value, CA.ssn
-- select count(*) --58284 -- select distinct CA.customText5
from bullhorn1.Candidate CA 
left join cf on cf.translate = CA.customText5
where CA.customText5 is not null and CA.customText5 <> ''
and CA.candidateID = 66209

SELECT top 300
         CA.candidateID as additional_id  
        , 'add_cand_info' as additional_type
        , 1005 as form_id
        , 1015 as field_id
        , ltrim(CA.ssn) as field_value
-- select count(*) --16114
from bullhorn1.Candidate CA 
where CA.ssn is not null and CA.ssn <> ''
and CA.candidateID = 66209

/*
drop table  cf
create table  cf (
translate	varchar(max),
field_id	int,
field_value	varchar(max) )
select * from cf
*/