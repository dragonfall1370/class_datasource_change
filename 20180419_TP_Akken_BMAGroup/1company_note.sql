#NOTE: SET group_concat_max_len = 2147483647;


DROP TABLE IF EXISTS truong_companynote;
CREATE TABLE IF NOT EXISTS truong_companynote as
       
       select 
                c.sno #,compstatus,industry,nbyears,ctype,department,nemployee,nloction,csource,csize,compbrief,comp_data,compsummary
              , group_concat(
                             case when (c.acc_comp = '' OR c.acc_comp = '0' OR c.acc_comp is NULL) THEN '' ELSE concat('Customer ID#: ',c.acc_comp,char(13)) END
                            ,case when (c.compstatus = '' OR c.compstatus = '0' OR c.compstatus is NULL) THEN '' ELSE concat('Status: ',status.name,char(13)) END      ###c.compstatus
                            ,case when (c.industry = '' OR c.industry = '0' OR c.industry is NULL) THEN '' ELSE concat('Industry: ',c.industry,char(13)) END
                            ,case when (c.nbyears = '' OR c.nbyears = '0' OR c.nbyears is NULL) THEN '' ELSE concat('Year Founded: ',c.nbyears,char(13)) END
                            ,case when (c.ctype = '' OR c.ctype = '0' OR c.ctype is NULL) THEN '' ELSE concat('Company Type: ',type.name,char(13)) END      ### c.ctype
                            ,case when (c.department = '' OR c.department = '0' OR c.department is NULL) THEN '' ELSE concat('Department: ',c.department,char(13)) END
                            ,case when (c.nemployee = '' OR c.nemployee = '0' OR c.nemployee is NULL) THEN '' ELSE concat('No. Employees: ',c.nemployee,char(13)) END
                            ,case when (c.nloction = '' OR c.nloction = '0' OR c.nloction is NULL) THEN '' ELSE concat('No. Locations: ',c.nloction,char(13)) END        ###
                            ,case when (c.csource = '' OR c.csource = '0' OR c.csource is NULL) THEN '' ELSE concat('Company Source: ',source.name,char(13)) END ### c.csource
                            ,case when (c.csize = '' OR c.csize = '0' OR c.csize is NULL) THEN '' ELSE concat('Company Size: ',c.csize,char(13)) END
                            #,case when (c.compbrief = '' OR c.compbrief = '0' OR c.compbrief is NULL) THEN '' ELSE concat('Company Brief: ',c.compbrief,char(13)) END
                            ,case when (c.comp_data = '' OR c.comp_data = '0' OR c.comp_data is NULL) THEN '' ELSE concat('Company Data: ',c.comp_data,char(13)) END
                            #,case when (c.compsummary = '' OR c.compsummary = '0' OR c.compsummary is NULL) THEN '' ELSE concat('Company Summary: ',c.compsummary,char(13)) END
                            ,case when (n0.notes = '' OR n0.notes = '0' OR n0.notes is NULL) THEN '' ELSE concat('Notes: ','\n',n0.notes,char(13)) END
               SEPARATOR '\n') as 'note'
       # select * #sno, compbrief, comp_data,  compsummary
       from staffoppr_cinfo c
       left join (select sno, type, name from manage where type = 'compstatus' ) status on status.sno = c.compstatus
       left join (select sno, type, name from manage where type = 'compsource' ) source on source.sno = c.csource
       left join (select sno, type, name from manage where type = 'comptype' ) type on type.sno = c.ctype
       ###left join (select * from staffoppr_location) location on location.csno = c.nloction
       left join (
                     select n.contactid, group_concat(concat('Date: ',n.cdate,'\n','By: ',u.name,'\n','Notes: ',n.notes,'\n') ORDER BY n.cdate desc SEPARATOR '\n') as notes 
                     from notes n 
                     left join staffoppr_cinfo c on c.sno = n.contactid 
                     left join (select username, name from users) u on u.username = n.cuser
                     #left join (select sno, type, name from manage where type = 'Notes' ) subtype on subtype.sno = n.notes_subtype #,'Sub Type: ',subtype.name
                     where c.sno is not null group by n.contactid ) n0 on n0.contactid = c.sno
                     group by c.sno;
                     

select * from truong_companynote;
