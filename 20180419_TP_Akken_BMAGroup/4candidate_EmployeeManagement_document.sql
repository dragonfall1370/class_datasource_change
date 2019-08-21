/*#NOTE: SET group_concat_max_len = 2147483647;

select count(*) from con_resumes
select count(*) from contact_doc 
select sno, username from empcon_general

select cd.sno, cd.con_id, cd.username, cd.title, cd.docname, el.sno,el.username ,el.name ,el.email
from contact_doc cd
left join emp_list el on el.sno = replace(cd.con_id,'emp','')
where el.sno is not null 
and el.sno in (1379)

select el.*, cg.*
# select count(*) # select distinct ltrim(rtrim(el.name))
from emp_list el
left join (select distinct email, concat(fname, ' ',lname) as name from candidate_general) cg  on cg.name = el.name # el.email = cg.email
where el.email  = '' and  
 el.sno = 1379;


                       select username, case when res_name not like '%.%' then concat(sno,'-',CONVERT(CAST(res_name as BINARY) USING utf8),'.rtf') else concat(sno,'-',CONVERT(CAST(res_name as BINARY) USING utf8) ) end as 'resume' #,type, status
                       from con_resumes
                       where username like 'cand%' and username in ('cand63678')

                       select 
                              con_id #, d.title, d.docname, d.doctype
                              ,concat(sno,'-',con_id,'-',replace(docname,',','')) as 'resume' 
                       from contact_doc 
                       where con_id like 'cand%' and con_id in ('cand63678')
                       
      select 
                cast(replace(con_id,'oppr','') as UNSIGNED) as sno
              , group_concat(concat(sno,'-',con_id,'-',replace(docname,',','')) SEPARATOR ',') as 'document'
       from contact_doc
       where con_id like 'oppr%' #and docname not like 'ContactPicture.jpg%' and docname <> 'Front.jpg' #and con_id like 'con2%'
*/


select distinct con_id from contact_doc where con_id like 'emp1379' #--<< Employee Management


-- without emails from Employee
SELECT 
        'CANDIDATE' as Sirius_entity_type
       , cg.username as Sirius_CandExtID
       , concat(cd.sno,'-',cd.con_id,'-',cd.docname) as Sirius_file_name
       , 'resume' as Sirius_document_type
       , 0 as Sirius_default_file
       , eg.sno, eg.username, eg.fname, eg.mname, eg.lname, eg.email, eg.profiletitle
from empcon_general eg
left join (select concat('cand',candidate) as candidate, username from empcon_jobs) ej on ej.username = eg.username
left join (select sno, username, fname, mname, lname, email, profiletitle from candidate_general) cg on cg.username = ej.candidate
left join (select * from contact_doc where con_id like 'emp%' ) cd on cd.con_id = concat('emp',eg.sno)
where cd.con_id is not null and cg.username is null
and cg.username in ('cand63678')


--  existing emails from Employee
SELECT 
        'CANDIDATE' as Sirius_entity_type
       , cg.username as Sirius_CandExtID
       , concat(cd.sno,'-',cd.con_id,'-',cd.docname) as Sirius_file_name
       , 'resume' as Sirius_document_type
       , 0 as Sirius_default_file
       , eg.sno, eg.username, eg.fname, eg.mname, eg.lname, eg.email, eg.profiletitle
from empcon_general eg
left join (select concat('cand',candidate) as candidate, username from empcon_jobs) ej on ej.username = eg.username
left join (select sno, username, fname, mname, lname, email, profiletitle from candidate_general) cg on cg.username = ej.candidate
left join (select * from contact_doc where  con_id like 'emp%' ) cd on cd.con_id = concat('emp',eg.sno)
where cd.con_id is not null and cg.username is not null
and cg.username in ('cand63678')

-----------------------

SELECT
         distinct cg.username as additional_id
        , 'add_cand_info' as additional_type
        , 1006 as form_id
        , 1016 as field_id
        , eg.sno
        , convert(eg.sno, char(10)) as field_value0
        , cast(eg.sno as char(10)) as field_value
        , eg.username, eg.fname, eg.mname, eg.lname, eg.email, eg.profiletitle
from empcon_general eg
left join (select concat('cand',candidate) as candidate, username from empcon_jobs) ej on ej.username = eg.username
left join (select sno, username, fname, mname, lname, email, profiletitle from candidate_general) cg on cg.username = ej.candidate
left join (select * from contact_doc where con_id like 'emp%' ) cd on cd.con_id = concat('emp',eg.sno)
where cd.con_id is not null and cg.username is not null 
and cg.username in ('cand63678','cand84709')
