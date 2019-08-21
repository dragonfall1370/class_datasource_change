
SET group_concat_max_len = 2147483647;


DROP TABLE IF EXISTS truong_candidatedoc;
CREATE TABLE IF NOT EXISTS truong_candidatedoc as
        select t.username, group_concat(t.resume SEPARATOR ',') as 'docs'
        from (
                       select username, case when res_name not like '%.%' then concat(sno,'-',CONVERT(CAST(res_name as BINARY) USING utf8),'.rtf') else concat(sno,'-',CONVERT(CAST(res_name as BINARY) USING utf8) ) end as 'resume' #,type, status
                       from con_resumes
                       where username like 'cand%' #and username in ('cand46838','cand55253')
                UNION ALL       
                       select 
                              con_id #, d.title, d.docname, d.doctype
                              ,concat(sno,'-',con_id,'-',replace(docname,',','')) as 'resume' 
                       from contact_doc 
                       where con_id like 'cand%' #and con_id in ('cand46838','cand55253')
               ) t
        where t.username <> '' and t.resume is not null
        group by t.username;
        
select count(*) FROM truong_candidatedoc;
select * from truong_candidatedoc where docs like '%cand63678%' and docs is not null;
select username from truong_candidatedoc group by username having count(*) > 1;


        

select
        cg.username as 'candidate-externalId'
       ,cg.profiletitle as 'Candidate-jobTitle1'
       ,case when (cg.fname = '' OR cg.fname is NULL) THEN 'No FirstName' ELSE cg.fname END as 'contact-firstName' #,cg.fname as 'candidate-firstName'
       ,cg.mname as 'candidate-MiddleName'
       ,case when (cg.lname = '' OR cg.lname is NULL) THEN 'No LastName' ELSE cg.lname END as 'contact-lastName' #,cg.lname as 'candidate-LastName'
       ,d.resume as 'candidate-resume'
from candidate_general cg
left join con_resumes r on r.username = cg.username
left join (
        select t.username, group_concat(t.resume SEPARATOR ',') as 'docs'
        from (
                       select username, case when res_name not like '%.%' then concat(res_name,'.rtf') else concat(sno,'-',res_name) end as 'resume' #,type, status
                       from con_resumes
                       #where username like 'cand%' and username in ('cand46838','cand55253')
                UNION ALL       
                       select 
                              con_id #, d.title, d.docname, d.doctype
                              ,concat(sno,'-',con_id,'-',replace(docname,',','')) as 'resume' 
                       from contact_doc 
                       #where con_id like 'cand%' and con_id in ('cand46838','cand55253')
               ) t
        where t.resume is not null
        group by t.username
       ) d on d.username = cg.username
where cg.username in ('cand55253')
or (cg.fname like 'Eliezer' and cg.lname like 'Vega') or (cg.fname like 'Jose' and cg.lname like '%Gonzalez Rivera')
 
 
select username from con_resumes group by username having count(*) > 1
select * from con_resumes where status != 'default' sno in (73386,100017,100018) 

select *  FROM contact_doc where con_id like '%63678%'
select con_id from contact_doc group by con_id having count(*) > 1

/*
       select username, case when res_name not like '%.%' then concat(res_name,'.rtf') else concat(sno,'-',res_name) end as 'resume' #,type, status
       from con_resumes
       where username like 'cand%' and username in ('cand46838','cand55253')
UNION ALL       
       select 
              con_id #, d.title, d.docname, d.doctype
              ,group_concat(concat(sno,'-',con_id,'-',replace(docname,',','')) SEPARATOR ',') as 'resume' 
       from contact_doc 
       #where con_id like 'cand%' and con_id in ('cand46838','cand55253')
       group by con_id
*/
