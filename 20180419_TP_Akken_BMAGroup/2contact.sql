# NOTE: SET group_concat_max_len = 2147483647;

-- NOTE
select c.sno #,c.fname,c.lname, c.mname, dontcall,dontemail,maincontact,address1,address2,city,state,zipcode,cat_id,source_name,cont_data,sourcetype,fax,other,other_extn,ctype,       accessto,reportto_name,email_3,spouse_name,fcompany,      description,other_info
       	, concat(
                      case when (c.dontcall = '' OR c.dontcall = '0' OR c.dontcall is NULL) THEN '' ELSE concat('Do Not Call Flag: ',c.dontcall,char(13)) END
                     ,case when (c.dontemail = '' OR c.dontemail = '0' OR c.dontemail is NULL) THEN '' ELSE concat('Do Not Email: ',c.dontemail,char(13)) END
                     ,case when (c.maincontact = '' OR c.maincontact = '0' OR c.maincontact is NULL) THEN '' ELSE concat('Display as Main Contact for Company: ',c.maincontact,char(13)) END
                     ,case when (c.address1 = '' OR c.address1 = '0' OR c.address1 is NULL) THEN '' ELSE concat('Address1: ',c.address1,char(13)) END
                     ,case when (c.address2 = '' OR c.address2 = '0' OR c.address2 is NULL) THEN '' ELSE concat('Address2: ',c.address2,char(13)) END
                     ,case when (c.city = '' OR c.city = '0' OR c.city is NULL) THEN '' ELSE concat('City: ',c.city,char(13)) END
                     ,case when (c.state = '' OR c.state = '0' OR c.state is NULL) THEN '' ELSE concat('State: ',c.state,char(13)) END
                     ,case when (c.zipcode = '' OR c.zipcode = '0' OR c.zipcode is NULL) THEN '' ELSE concat('Zip: ',c.zipcode,char(13)) END
                     ,case when (c.country = '' OR c.country = '0' OR c.country is NULL) THEN '' ELSE concat('Country: ',countries.country,char(13)) END ###c.country
                     ,case when (c.cat_id = '' OR c.cat_id = '0' OR c.cat_id is NULL) THEN '' ELSE concat('Category: ',cat.name,char(13)) END ###c.cat_id
                     ,case when (c.source_name = '' OR c.source_name = '0' OR c.source_name is NULL) THEN '' ELSE concat('Source: ',c.source_name,char(13)) END
                     ,case when (c.cont_data = '' OR c.cont_data = '0' OR c.cont_data is NULL) THEN '' ELSE concat('Description: ',c.cont_data,char(13)) END
                     ,case when (c.sourcetype = '' OR c.sourcetype = '0' OR c.sourcetype is NULL) THEN '' ELSE concat('Source Type: ',source.name,char(13)) END ###c.sourcetype
                     ,case when (c.fax = '' OR c.fax = '0' OR c.fax is NULL) THEN '' ELSE concat('Fax: ',c.fax,char(13)) END
                     ,case when (c.other = '' OR c.other = '0' OR c.other is NULL) THEN '' ELSE concat('Other: ',c.other,char(13)) END
                     ,case when (c.other_extn = '' OR c.other_extn = '0' OR c.other_extn is NULL) THEN '' ELSE concat('Other: ',c.other_extn,char(13)) END
                     ,case when (c.ctype = '' OR c.ctype = '0' OR c.ctype is NULL) THEN '' ELSE concat('Contact Type: ',type.name,char(13)) END ###ctype
                     #,case when (c.Groups = '' OR c.Groups = '0' OR c.Groups is NULL) THEN '' ELSE concat('Groups: ',c.Groups,char(13)) END
                     ,case when (c.deptid = '' OR c.deptid = '0' OR c.deptid is NULL) THEN '' ELSE concat('HRM Department: ',dept.deptname,char(13)) END #WRONG#c.accessto
                     ,case when (c.reportto_name = '' OR c.reportto_name = '0' OR c.reportto_name is NULL) THEN '' ELSE concat('Reports To: ',c.reportto_name,char(13)) END
                     ,case when (c.email_3 = '' OR c.email_3 = '0' OR c.email_3 is NULL) THEN '' ELSE concat('Other Email: ',c.email_3,char(13)) END
                     ,case when (c.spouse_name = '' OR c.spouse_name = '0' OR c.spouse_name is NULL) THEN '' ELSE concat('Spouse: ',c.spouse_name,char(13)) END
                     ,case when (c.fcompany = '' OR c.fcompany = '0' OR c.fcompany is NULL) THEN '' ELSE concat('Former Companies: ',c.fcompany,char(13)) END
                     #,case when (c.bill_burden_type = '' OR c.bill_burden_type = '0' OR c.bill_burden_type is NULL) THEN '' ELSE concat('Bill Burden: ',c.bill_burden_type,char(13)) END
                     ,case when (c.description = '' OR c.description = '0' OR c.description is NULL) THEN '' ELSE concat('Contact Note: ',c.description,char(13)) END
                     ,case when (c.other_info = '' OR c.other_info = '0' OR c.other_info is NULL) THEN '' ELSE concat('Contact Note: ',c.other_info,char(13)) END
        ) as 'note'
# select count(*)
from staffoppr_contact c
left join countries on countries.sno = c.country
left join (select sno, type, name from manage where type = 'compsource' ) source on source.sno = c.sourcetype
left join (select sno, type, name from manage where type = 'contacttype' ) type on type.sno = c.ctype
left join (select sno, type, name from manage where type = 'Category' ) cat on cat.sno = c.cat_id
left join (select sno, deptname from department ) dept on dept.sno = c.deptid
where accessto = 'ALL'
or c.fname like '%Kathyuska%' or c.lname like '%Acuna%' or mname like '%Acuna%' 


select * from staffoppr_contact c 
where c.fname in ('Mari' )
or c.lname in ('Acuna','Debolin')
or mname like '%Acuna%' 
or c.sno in (1754)

select * from staffacc_location


-- COMMENT
select * from staffoppr_oppr_his



-- OWNER

/*
select distinct c.accessto, e.email #distinct users.name as 'contact-owners' , e.email
from staffoppr_contact c
left join (select username, email from emp_list where email is not null and email <> '' ) e on e.username = c.accessto
select distinct c.accessto from staffoppr_contact c */

create table TRUONG_contactowneremail as 
select distinct c.accessto, e.email #distinct users.name as 'company-owners' , e.email
from staffoppr_contact c
left join (select username, name from users) users on users.username = c.accessto
left join (select distinct ltrim(rtrim(name)) as name, email from emp_list where email is not null and email <> '' ) e on e.name = users.name ;




-- MAIN SCRIPT
select
        case when (c.fname = '' OR c.fname is NULL) THEN 'No FirstName' ELSE c.fname END as 'contact-firstName' #c.fname
       ,c.mname as 'contact-middleName'
       ,case when (c.lname = '' OR c.lname is NULL) THEN 'No LastName' ELSE c.lname END as 'contact-lastName' #,c.lname
       ,IF(c.email = '',concat('contact_',c.sno,'@noemail.com'),case when tc.num > 1 THEN concat(tc.email,'_',tc.num) ELSE tc.email END ) as 'contact-email' #,c.email 
,c.csno as 'contact-companyId'
       ,c.messengerid  as 'contact-skype'
       #,c.wphone, c.wphone_extn as 'contact-phone'
       #,c.hphone, c.hphone_extn as 'contact-workphone'
              ,ltrim(substring(concat(
                        case when (c.wphone = '' OR c.wphone is NULL) THEN '' ELSE concat(', ',c.wphone) END
                       ,case when (c.wphone_extn = '' OR c.wphone_extn is NULL) THEN '' ELSE concat(', ',c.wphone_extn) END
                       ),2)) as 'contact-phone'
              ,ltrim(substring(concat(
                        case when (c.hphone = '' OR c.hphone is NULL) THEN '' ELSE concat(', ',c.hphone) END
                       ,case when (c.hphone_extn = '' OR c.hphone_extn is NULL) THEN '' ELSE concat(', ',c.hphone_extn) END
                       ),2)) as 'contact-homephone'         
       ,c.mobile as 'contact-mobile'
       ,c.ytitle as 'contact-jobTitle'
       ,department.name as 'contact-deparment' #,c.department
       ,c.email_2  as 'contact-personalEmail'
#,csno to link to staffoppr_cinfo table
       ,c.sno as 'Contact-ExternalID'
       ,note.note as 'contact-note'
       ,o.email as 'contact-owners'
       ,d.document as 'contact-document'
# select count(*)
from staffoppr_contact c
left join truong_contact tc on tc.sno = c.sno
left join TRUONG_companyowneremail o on o.owner = c.accessto
left join truong_contactdocument d on d.sno = c.sno
#left join (select * from truong_companynote where note is not null) n on n.sno = c.sno
left join (select sno, type, name from manage where type = 'Department' ) department on department.sno = c.department
left join truong_contactnote note on note.sno = c.sno
#where d.document is not null
#c.sno in (2961) #c.sno like '2%' and 
#where c.accessto = 'ALL'


/*

select
       c.sno as 'Contact-ExternalID'
       , case when department.name is null then '' else department.name end as 'contact-deparment' #,c.department
       ,c.email_2  as 'contact-personalEmail'
# select *
from staffoppr_contact c
left join (select sno, type, name from manage where type = 'Department' ) department on department.sno = c.department
where department.name <> '' or c.email_2 <> ''

*/