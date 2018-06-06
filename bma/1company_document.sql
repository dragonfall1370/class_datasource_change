#NOTE: SET group_concat_max_len = 2147483647;
select * from contact_doc where docname like '%BCK Pathstone Agreement 2018.pdf%'
select * from contact_doc  where con_id = 'com2176'

select c.sno as 'company-externalId',c.cname, d.document as 'company-document'
from staffoppr_cinfo c
left join truong_companydocument d on d.sno = c.sno
where c.sno in (1870)

-----------------------


DROP TABLE IF EXISTS truong_companydocument;
CREATE TABLE IF NOT EXISTS truong_companydocument as
       select 
                cast(replace(con_id,'com','') as UNSIGNED) as sno
              , group_concat(concat(sno,'-',con_id,'-',replace(docname,',','')) SEPARATOR ',') as 'document'
       from contact_doc
       where con_id like 'com%'
       group by con_id
       #where con_id = 'com2176'
       ;
       
select * from truong_companydocument where document like '%Executed 2016 Regional NIT Unilever.pdf%'
       
       