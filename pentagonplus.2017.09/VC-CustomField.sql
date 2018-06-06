select top 100 CandidateId, Discipline from candidates c
select distinct Discipline from candidates c

with t as (
        select top 100 
        CandidateId,
        case Discipline
                when 'Supply Chain' then 'Logistics Distribution and Supply Chain'
                when 'Sales Marketing' then 'Sales & Marketing'
                when 'Others' then 'Others'
                when 'IT/Telco' then 'IT & Telecoms'
                when 'HR' then 'HR, GA & Facilities'
                when 'Finance' then 'Accounting & Finance'
                when 'Engineering' then 'Engineering'
                when 'Banking & Insurance' then 'Banking & Insurance'
        end as 'FE'
        , Discipline
        from candidates c )
select distinct FE from t

with t as (
        select
        CandidateId as candidate_id
        , concat(FirstName,' ',LastName) as fullname
        , case Discipline
            when 'Finance' then 3093
            when 'Banking & Insurance' then 2985
            when 'Engineering' then 3097
            when 'HR' then 3099
            when 'IT/Telco' then 3100
            when 'Supply Chain' then 2983
            when 'Others' then 2984
            when 'Sales Marketing' then 3106
        end as 'functional_expertise_id'
        from candidates )
-- select count(*) from t        
select candidate_id, fullname, functional_expertise_id from t where candidate_id in ('Zrecruit_139304000000168699','Zrecruit_139304000000170229')

SELECT top 300
         CA.candidateID as additional_id  
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