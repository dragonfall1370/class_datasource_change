#NOTE: SET group_concat_max_len = 2147483647;

select count(*) from con_resumes
select count(*) from contact_doc 
select * from contact_doc where con_id = 'emp1379' --<< Employee Management
select * from contact_doc where docname like '%Evaluacion Mario Font Dic 2017.pdf%'
select docname from contact_doc  where con_id = 'con29017'
-----------------------

       
DROP TABLE IF EXISTS truong_contactdocument;
CREATE TABLE IF NOT EXISTS truong_contactdocument as
       select 
                cast(replace(con_id,'oppr','') as UNSIGNED) as sno
              , group_concat(concat(sno,'-',con_id,'-',replace(docname,',','')) SEPARATOR ',') as 'document'
       from contact_doc
       where con_id like 'oppr%' #and docname not like 'ContactPicture.jpg%' and docname <> 'Front.jpg' #and con_id like 'con2%'
       group by con_id
       ;
              
select * from truong_contactdocument;
select count(*) from truong_contactdocument;