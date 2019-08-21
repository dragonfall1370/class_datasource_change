#NOTE: SET group_concat_max_len = 2147483647;

select count(*) from con_resumes
select count(*) from contact_doc 
select * from contact_doc where docname like '%BCK Pathstone Agreement 2018.pdf%'
select docname from contact_doc  where con_id = 'com2176'
----------------------
       
       
DROP TABLE IF EXISTS truong_jobdocument;
CREATE TABLE IF NOT EXISTS truong_jobdocument as
       select 
                cast(replace(con_id,'req','') as UNSIGNED) as sno
              , group_concat(concat(sno,'-',con_id,'-',replace(docname,',','')) SEPARATOR ',') as 'document'
       from contact_doc
       where con_id like 'req%' #and con_id like 'con2%'
       group by con_id
       ;
              
select * from truong_jobdocument;
select count(*) from truong_jobdocument;