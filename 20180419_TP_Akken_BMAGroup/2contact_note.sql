SET group_concat_max_len = 2147483647;

#select * from staffoppr_contact c where sno like '%944%'

DROP TABLE IF EXISTS truong_contactnote;
CREATE TABLE truong_contactnote as 
       select c.sno #,c.fname,c.lname, c.mname, dontcall,dontemail,maincontact,address1,address2,city,state,zipcode,cat_id,source_name,cont_data,sourcetype,fax,other,other_extn,ctype,       accessto,reportto_name,email_3,spouse_name,fcompany,      description,other_info
                     , group_concat(
                             case when (c.dontcall = '' OR c.dontcall = '0' OR c.dontcall is NULL) THEN '' ELSE concat('Do Not Call Flag: ',c.dontcall,char(13)) END
                            ,case when (c.dontemail = '' OR c.dontemail = '0' OR c.dontemail is NULL) THEN '' ELSE concat('Do Not Email: ',c.dontemail,char(13)) END
                            ,case when (c.maincontact = '' OR c.maincontact = '0' OR c.maincontact is NULL) THEN '' ELSE concat('Display as Main Contact for Company: ',c.maincontact,char(13)) END
                            ,case when (c.address1 = '' OR c.address1 = '0' OR c.address1 is NULL) THEN '' ELSE concat('Address1: ',c.address1,char(13)) END
                            ,case when (c.address2 = '' OR c.address2 = '0' OR c.address2 is NULL) THEN '' ELSE concat('Address2: ',c.address2,char(13)) END
                            ,case when (c.city = '' OR c.city = '0' OR c.city is NULL) THEN '' ELSE concat('City: ',c.city,char(13)) END
                            ,case when (c.state = '' OR c.state = '0' OR c.state is NULL) THEN '' ELSE concat('State: ',c.state,char(13)) END
                            ,case when (c.zipcode = '' OR c.zipcode = '0' OR c.zipcode is NULL) THEN '' ELSE concat('Zip: ',c.zipcode,char(13)) END
                            ,case when (c.country = '' OR c.country = '0' OR c.country is NULL) THEN '' ELSE concat('Country: ',countries.country,char(13)) END ###c.country
                            ,case when (c.cat_id = '' OR c.cat_id = '0' OR c.cat_id is NULL) THEN '' ELSE concat('Category: ',replace(replace(replace(replace(replace(c.cat_id,'298','Red Category'),'304','Seasonal'),'942','My Contacts'),'944','Most Contacted'),'1314','Reporting Manager'),char(13)) END ###c.cat_id
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
               SEPARATOR '\n') as 'note'
       # select count(*)
       from staffoppr_contact c
       left join countries on countries.sno = c.country
       left join (select sno, type, name from manage where type = 'compsource' ) source on source.sno = c.sourcetype
       left join (select sno, type, name from manage where type = 'contacttype' ) type on type.sno = c.ctype
       #left join (select sno, type, name from manage where type = 'Category' ) cat on cat.sno = c.cat_id
       left join (select sno, deptname from department ) dept on dept.sno = c.deptid
       #where c.sno like '2%' #and c.sno in (214,215,216,217,218)
       group by c.sno
;

select * from truong_contactnote;