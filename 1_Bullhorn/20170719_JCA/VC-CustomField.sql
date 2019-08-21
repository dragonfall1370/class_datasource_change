
-- CANDIDATE
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


-- CONTACT
-- Last Contacted Date
select top 100
        Cl.clientID as additional_id
        , 'add_con_info' as additional_type
        , 1004 as form_id
        , 1015 as field_id
        , UC.dateAdded as field_date_value        
from bullhorn1.BH_UserContact UC
left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
where Cl.isPrimaryOwner = 1 and Cl.clientID = 6899 --and Cl.isDeleted = 0

-- Bullhorn Added Date
select top 100
        Cl.clientID as additional_id
        , 'add_con_info' as additional_type
        , 1004 as form_id
        , 1016 field_id
        , UC.dateLastComment as field_date_value        
from bullhorn1.BH_UserContact UC
left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
where Cl.isPrimaryOwner = 1 and Cl.clientID = 6899 --and Cl.isDeleted = 0