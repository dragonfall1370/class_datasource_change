# NOTE: SET group_concat_max_len = 2147483647;

DROP TABLE IF EXISTS truong_candidatenote;
CREATE TABLE IF NOT EXISTS truong_candidatenote as
       select cg.username, cg.fname, cg.mname, cg.lname,
              group_concat(
                      case when (cat.name is NULL) THEN '' ELSE concat('Position Category: ',cat.name,char(13)) END #cg.jobcatid
                     ,case when (cg.cg_source = '' OR cg.cg_source = '0' OR cg.cg_source is NULL) THEN '' ELSE concat('Source: ',cg.cg_source,char(13)) END
                     ,case when (cg.fax = '' OR cg.fax = '0' OR cg.fax is NULL) THEN '' ELSE concat('Fax: ',cg.fax,char(13)) END
                     ,case when (cg.other = '' OR cg.other = '0' OR cg.other is NULL) THEN '' ELSE concat('Other: ',cg.other,char(13)) END
                     ,case when (cg.other_extn = '' OR cg.other_extn = '0' OR cg.other_extn is NULL) THEN '' ELSE concat('Other Ext: ',cg.other_extn,char(13)) END
                     
                     ,if ('Contact Method: ' =
                                   concat('Contact Method: ',
                                   case cg.cphone when 'TRUE' then 'Phone ' when 'FALSE' then '' when null then '' when '' then '' end
                                   ,case cg.cmobile when 'TRUE' then 'Mobile ' when 'FALSE' then '' when null then '' when '' then '' end
                                   ,case cg.cfax when 'TRUE' then 'Fax ' when 'FALSE' then '' when null then '' when '' then '' end
                                   ,case cg.cemail when 'TRUE' then 'Email' when 'FALSE' then '' when null then '' when '' then '' end )
                            ,''
                            ,concat('Contact Method: ',
                            case cg.cphone when 'TRUE' then 'Phone ' when 'FALSE' then '' when null then '' when '' then '' end
                            ,case cg.cmobile when 'TRUE' then 'Mobile ' when 'FALSE' then '' when null then '' when '' then '' end
                            ,case cg.cfax when 'TRUE' then 'Fax ' when 'FALSE' then '' when null then '' when '' then '' end
                            ,case cg.cemail when 'TRUE' then 'Email' when 'FALSE' then '' when null then '' when '' then '' end )
                            )
                     
                     ,case when (prof.objective is NULL or prof.objective = '') THEN '' ELSE concat('Objective: ',prof.objective,char(13)) END
                     ,case when (prof.summary is NULL or prof.summary = '') THEN '' ELSE concat('Summary: ',replace(prof.summary,'Â',''),char(13)) END
                     
                     ,case when (pref.desirejob is NULL or pref.desirejob = '||') THEN '' ELSE concat('Desired Job Type: ',pref.desirejob,char(13)) END
                     ,case when (pref.desirestatus is NULL or pref.desirestatus = '|') THEN '' ELSE concat('Desired Employment Type: ',pref.desirestatus,char(13)) END
                     ,case when (pref.desirelocation is NULL or pref.desirelocation = '') THEN '' ELSE concat('Desired Job Location Type: ',pref.desirelocation,char(13)) END
                     
                     ,case pref.resourcetype
                            when 'true^true' then concat('Desired Resource Type: Independent Contractor, Payrolled Employee',char(13))
                            when 'true^false' then concat('Desired Resource Type: Independent Contractor',char(13))
                            when 'false^true' then concat('Desired Resource Type: Payrolled Employee',char(13))
                            when 'false^false' then ''
                            when NULL THEN ''
                            when '' THEN ''
                            END
                     #,case when (pref.status is NULL) THEN '' ELSE concat('Present Status: ',pref.status,char(13)) END
                     
                     ,case when (pref.amount is NULL or pref.amount = '') THEN '' ELSE concat('Desired Salary: ',pref.amount,' / ',pref.currency,' / ',pref.period,char(13)) END
                     #,case when (pref.currency is NULL) THEN '' ELSE concat('Desired Salary Currency: ',pref.currency,char(13)) END
                     #,case when (pref.period is NULL) THEN '' ELSE concat('Desired Salary Amount Period: ',pref.period,char(13)) END
                     ,case when (pref.compcomments is NULL or pref.compcomments = '') THEN '' ELSE concat('Desired Salary Comments: ',pref.compcomments,char(13)) END
                     
                     ,case when (pref.rperiod is NULL) THEN '' ELSE concat('Rate - Amount Period: ',pref.rperiod,' / ',pref.rcurrency,char(13)) END
                     #,case when (pref.rcurrency is NULL) THEN '' ELSE concat('Rate - Currency: ',pref.rcurrency,char(13)) END
                     
                     ,case when (pref.pramount is NULL) THEN '' ELSE concat('Rate - Pay - Regular Amount: ',pref.pramount,'. Over Time Amount: ',pref.poamount,char(13)) END
                     #,case when (pref.poamount is NULL) THEN '' ELSE concat('Rate - Pay - Over Time Amount: ',pref.poamount,char(13)) END
                     
                     ,case when (pref.iramount is NULL) THEN '' ELSE concat('Rate - Min Pay - Regular Amount: ',pref.iramount,'. Over Time Amount: ',pref.ioamount,char(13)) END
                     #,case when (pref.ioamount is NULL) THEN '' ELSE concat('Rate - Min Pay - Over Time Amount: ',pref.ioamount,char(13)) END
                     
                     ,case when (pref.aramount is NULL) THEN '' ELSE concat('Rate - Max Pay - Regular Amount: ',pref.aramount,'. Over Time Amount: ',pref.aoamount,char(13)) END
                     #,case when (pref.aoamount is NULL) THEN '' ELSE concat('Rate - Max Pay - Over Time Amount: ',pref.aoamount,char(13)) END
                     
                     ,case pref.wtravle when 'true' then concat('Willing to Travel: Yes',char(13)) when 'false' then concat('Willing to Travel: No',char(13)) else '' END
                     ,case when (pref.ptravle is NULL or pref.ptravle = '') THEN '' ELSE concat('Percentage Travel: ',pref.ptravle,' % of time',char(13)) END
                     
                     ,case when (pref.tcomments is NULL or pref.tcomments = '') THEN '' ELSE concat('Travel Considerations: ',pref.tcomments,char(13)) END
                     
                     ,case pref.wlocate when 'true' then concat('Willing to Relocate: Yes',char(13)) when 'false' then concat('Willing to Relocate: No',char(13)) else '' END
                     ,case when (pref.city is NULL) THEN '' ELSE concat('Willing to Relocate:',' City: ',pref.city,'State: ',pref.state,'Country: ',countries.country,char(13)) END
                     #,case when (pref.state is NULL or pref.state = '') THEN '' ELSE concat('Willing to Relocate - State: ',pref.state,char(13)) END
                     #,case when (pref.country is NULL or pref.country = '') THEN '' ELSE concat('Willing to Relocate - Country: ',pref.country,char(13)) END
                     ,case when (pref.lcomments is NULL or pref.lcomments = '') THEN '' ELSE concat('Willing to Relocate - Comments: ',pref.lcomments,char(13)) END
                     
                     ,case when (pref.tmax is NULL or pref.tmax = '') THEN '' ELSE concat('Commute Information - Time Max: ',pref.tmax,char(13)) END
                     ,case when (pref.dmax is NULL or pref.dmax = '') THEN '' ELSE concat('Commute Information - Distance Max: ',pref.dmax,char(13)) END
                     ,case when (pref.ccomments is NULL or pref.ccomments = '') THEN '' ELSE concat('Commute Information - Comments: ',pref.ccomments,char(13)) END
                     
                     ,case when (list.avail is NULL) THEN '' ELSE concat('Availability: ',list.avail,char(13)) END
                     ,case when (list.dontemail is NULL) THEN '' ELSE concat('Do Not Email: ',list.dontemail,char(13)) END
                     #,case when (list.body is NULL) THEN '' ELSE concat('Body: ',list.body,char(13)) END
                     #,case when (list.filecontent is NULL) THEN '' ELSE concat('File Content: ',list.filecontent,char(13)) END
               SEPARATOR '\n\n') as 'note'
       # select count(*) #98389# select *
       from candidate_general cg 
       left join candidate_prof prof on prof.username = cg.username
       left join candidate_pref pref on pref.username = cg.username
       #left join applicants app on app.username = cg.username
       left join (
              select username,
                     group_concat(
                      case when (affcname is NULL) THEN '' ELSE concat('Affiliations - Company Name: ',affcname,char(13)) END
                     ,case when (affrole is NULL or affrole = '') THEN '' ELSE concat('Affiliations - Role: ',affrole,char(13)) END
                     ,case when (affsdate is NULL or affsdate = '') THEN '' ELSE concat('Affiliations - Start Date: ',affsdate,char(13)) END
                     ,case when (affedate is NULL or affedate = '') THEN '' ELSE concat('Affiliations - End Date: ',affedate,char(13)) END
                     SEPARATOR '\n\n') as 'note'
              from candidate_aff
              group by username              
              ) aff on aff.username = cg.username
       left join (
              select username,
                     group_concat('References: '
                            ,case when (name is NULL) THEN '' ELSE concat('Name: ',name,char(13)) END
                            ,case when (company is NULL or company = '') THEN '' ELSE concat('Company: ',company,char(13)) END
                            ,case when (title is NULL or title = '') THEN '' ELSE concat('Title: ',title,char(13)) END
                            ,case when (phone is NULL or phone = '') THEN '' ELSE concat('Phone: ',phone,char(13)) END
                            ,case when (secondary is NULL or secondary = '') THEN '' ELSE concat('Secondary Phone: ',secondary,char(13)) END
                            ,case when (mobile is NULL or mobile = '') THEN '' ELSE concat('Mobile: ',mobile,char(13)) END
                            ,case when (email is NULL or email = '') THEN '' ELSE concat('Email: ',email,char(13)) END
                            ,case when (notes is NULL or notes = '') THEN '' ELSE concat('Notes: ',notes,char(13)) END
                     SEPARATOR '\n\n') as 'note'
              from candidate_ref
              group by username
              ) re on re.username = cg.username
       left join candidate_list list on list.username = cg.username
       left join (select sno, type, name from manage where type = 'candjobcat' ) cat on cat.sno = cg.jobcatid
       left join countries on countries.sno = pref.country
       #where cg.username in ('cand15863','cand44582','cand45000','cand11939','cand45000') # and pref.resourcetype <> ''
       group by cg.username
;


 select count(*) from truong_candidatenote where note is not null;
# select * from truong_candidatenote where note is not null;