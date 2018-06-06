# NOTE:
# SET group_concat_max_len = 2147483647;

-- OWNER
drop table TRUONG_companyowneremail;
create table TRUONG_companyowneremail as 
select distinct c.owner ,users.name ,e.email
from staffoppr_cinfo c
left join (select username, name from users) users on users.username = c.owner
left join (select distinct ltrim(rtrim(name)) as name, email from emp_list where email is not null and email <> '' and email <> 'arletteassam@yahoo.com') e on e.name = users.name 
;


-- ACTIVITIES COMMENTS
select 
        com.sno as 'externalId' ,com.cname
       ,-10 as 'user_account_id'
       ,'comment' as 'category'
       ,'company' as 'type'
       ,n.cdate as 'insert_timestamp'
       ,concat(
              'User: ',u.name,'\n'
              ,case when st.name is NULL THEN '' ELSE concat('Sub Type: ',st.name,'\n') END
              ,concat('Title: ',n.notes)
              ) as 'content'
# select count(*)
from notes n
left join staffoppr_cinfo com on com.sno = n.contactid
left join users u on u.username = n.cuser
left join (select sno, type, name from manage where type = 'Notes' ) st on st.sno = n.notes_subtype
where n.type = 'com' and com.cname like ('%Embassy%') or com.sno in (1237)
order by n.cdate desc 

select * from notes n where n.type = 'com'  and n.notes = ''


-- DOCUMENT
select 
        com.sno, com.cname
       ,u.name
       ,cd.* 
from contact_doc cd
left join staffoppr_cinfo com on concat('com',com.sno) = cd.con_id
left join users u on u.username = cd.username
where com.sno = 1237




-- MAIN SCRIPT --

select
        c.sno as 'company-externalId'
       ,o.email as 'company-owners'  #,users.name as 'company-owners' ,c.owner 
       #,c.cname as 'company-name'
       ,IF(c.cname = '',concat(tc.sno," No CompanyName ",tc.num),case when tc.num > 1 THEN concat(tc.cname,' ',tc.num) ELSE tc.cname END ) as 'company-name'
       ,c.curl as 'company-website'
       ,c.phone as 'company-phone'
       ,c.fax as 'company-fax'
       ,c.city as 'company-locationCity'
       ,c.state as 'company-locationState'
       ,c.zip as 'company-locationZipCode'
       ,countries.country_abbr as 'company-locationCountry'
       ,ltrim(substring(concat(
                 case when (c.Address1 = '' OR c.Address1 is NULL) THEN '' ELSE concat(' ',c.Address1) END
                ,case when (c.Address2 = '' OR c.Address2 is NULL) THEN '' ELSE concat(', ',c.Address2) END
                ,case when (c.city = '' OR c.city is NULL) THEN '' ELSE concat(', ',c.city) END
                ,case when (c.state = '' OR c.state is NULL) THEN '' ELSE concat(', ',c.state) END
                ,case when (c.zip = '' OR c.zip is NULL) THEN '' ELSE concat(', ',c.zip) END
                ,case when (countries.country = '' OR countries.country is NULL) THEN '' ELSE concat(', ',countries.country) END
                ),2)) as 'company-locationaddress'
       ,ltrim(substring(concat(
                 case when (c.city = '' OR c.city is NULL) THEN '' ELSE concat(', ',c.city) END
                ,case when (c.state = '' OR c.state is NULL) THEN '' ELSE concat(', ',c.state) END
                ,case when (c.zip = '' OR c.zip is NULL) THEN '' ELSE concat(', ',c.zip) END
                ,case when (countries.country = '' OR countries.country is NULL) THEN '' ELSE concat(', ',countries.country) END
                ),2)) as 'company-locationName'        
       , n.note as 'company-note'
       ,d.document as 'company-document'
# select count(*) #3331 # select *
from staffoppr_cinfo c
left join truong_company tc on tc.sno = c.sno
#left join (select username, name from users) users on users.username = c.owner
left join truong_companyowneremail o on o.owner = c.owner
left join truong_companydocument d on d.sno = c.sno
left join countries on countries.sno = c.country
left join (select * from truong_companynote where note is not null) n on n.sno = c.sno

where c.sno in (1064,1296, 3260, 3233)

