
# NOTE:
SET group_concat_max_len = 2147483647;

-- OWNER
drop table TRUONG_companyowneremail;
create table TRUONG_companyowneremail as 
select distinct c.owner ,users.name ,e.email
from candidate_list c
left join (select username, name from users) users on users.username = c.owner
left join (select distinct ltrim(rtrim(name)) as name, email from emp_list where email is not null and email <> '' and email <> 'arletteassam@yahoo.com') e on e.name = users.name 
select * from TRUONG_companyowneremail;

-- DOCUMENT
select * from candidate_general cg where username in ('cand29613')

select 
        cg.username, cg.profiletitle, cg.fname, cg.mname, cg.lname, cg.email
       ,u.name
       ,cd.*
from contact_doc cd
left join candidate_general cg on cg.username = cd.con_id
left join users u on u.username = cd.username
where cg.username in ('cand30789')


-- EDUCATION
       SELECT 
                         @rn := case when @username = username then @rn + 1 else 1 end AS rn,
                         @username := username as username,
       heducation as 'Education > School or Program Name'
       ,edudegree_level as 'Education > Degree/Level Attained'
       ,edudate as 'Education > Completion Date'
from candidate_edu e
ORDER BY username, sno DESC


-- EMAIL
select 
        cg.username, cg.profiletitle, cg.fname, cg.mname, cg.lname, cg.email
       ,u.name
       ,ce.*
from contact_email ce
left join candidate_general cg on cg.username = ce.username --<<<
left join users u on u.username = ce.username
where cg.username like '%cand29613%' 

select * from contact_email where contactsno like '%cand29613%' and subject like '%Rv: BRENDA MARRERO%' 
select * from mail_headers where subject like '%Rv: BRENDA MARRERO%'


-- ACTIVITIES
select 
        cg.username as 'externalId' ,cg.fname,cg.mname,cg.lname
       ,-10 as 'user_account_id'
       ,'comment' as 'category'
       ,'candidate' as 'type'
       ,n.cdate as 'insert_timestamp'
       ,concat(
              'User: ',u.name,'\n'
              ,case when st.name is NULL THEN '' ELSE concat('Sub Type: ',st.name,'\n') END
              ,concat('Title: ',n.notes)
              ) as 'content'
# select count(*)
from notes n
left join candidate_general cg on cg.username = concat(n.type,n.contactid)
left join users u on u.username = n.cuser
left join (select sno, type, name from manage where type = 'Notes' ) st on st.sno = n.notes_subtype
where n.type = 'cand' and cg.username like ('cand29613')
order by n.cdate desc 

select * from notes n where n.type = 'cand'  and n.notes = ''


-- MAIN ACTIVITIES
select 
        cp.con_id as 'externalId' ,cg.fname,cg.mname,cg.lname ,cp.sno
       ,-10 as 'user_account_id'
       ,'comment' as 'category'
       ,'candidate' as 'type'
       ,cp.sdate as 'insert_timestamp'
       ,concat(
              'User: ',u.name,'\n'
              ,'Title: ',cp.title,'\n'
              ,'Subject: ',cp.subject,'\n'
              ,case when note.subtype is NULL or note.subtype = '' THEN '' ELSE concat('Sub Type: ',note.subtype,'\n') END
              ,case when note.notes is NULL or note.notes = '' THEN '' ELSE concat('Notes: ',note.notes,'\n') END
              ) as 'content'
# select count(*)
from cmngmt_pr cp
left join candidate_general cg on cg.username = cp.con_id
left join users u on u.username = cp.username
left join (
        #-- Notes
                select n.sno,'' as notes, st.name as subtype
                from notes n
                left join (select sno, type, name from manage where type = 'Notes' ) st on st.sno = n.notes_subtype
                where n.type = 'cand' #and n.contactid = 29613
        union all
        #-- Submitted
                select sno, enotes as notes,'' as subtype
                from contact_event #where con_id = 'cand29613'
       union all 
       #-- Document
                select sno, notes, '' as subtype
                from contact_doc #where con_id = 'cand29613'
) note on note.sno = cp.tysno
#where cp.con_id like ('cand29613')
#order by cp.sdate desc 

-- MAIN SCRIPT
select
        cg.username as 'candidate-externalId'
       ,cg.profiletitle as 'Candidate-jobTitle1'
       ,cg.fname as 'candidate-firstName'
       ,cg.mname as 'candidate-MiddleName'
       ,cg.lname as 'candidate-LastName'
       ,cg.alternate_email as 'candidate-PersonalEmail'
       ,group_concat(cg.email,cg.other_email SEPARATOR ',') as 'candidate-email'     
       ,source.name as 'candidate-Source' #cg.cg_sourcetype
       ,cg.mobile as 'candidate-mobile'
       ,o.email as 'Candidate Owners'
       ,ltrim(substring(concat(
                 case when (cg.address1 = '' OR cg.address1 is NULL) THEN '' ELSE concat(', ',cg.address1) END
                ,case when (cg.address2 = '' OR cg.address2 is NULL) THEN '' ELSE concat(', ',cg.address2) END
                ,case when (cg.city = '' OR cg.city is NULL) THEN '' ELSE concat(', ',cg.city) END
                ,case when (cg.state = '' OR cg.state is NULL) THEN '' ELSE concat(', ',cg.state) END
                ,case when (cg.zip = '' OR cg.zip is NULL) THEN '' ELSE concat(', ',cg.zip) END
                ,case when (countries.country = '' OR countries.country is NULL) THEN '' ELSE concat(', ',countries.country) END
                ),2)) as 'candidate-address'                
    	, cg.city as 'candidate-city'
	, cg.state as 'candidate-state'
	, cg.zip as 'candidate-zipCode'
	,countries.country_abbr as 'candidate-country' #cg.country
       ,ltrim(substring(concat(
               case when (cg.hphone = '' OR cg.hphone is NULL) THEN '' ELSE concat(', ',cg.hphone) END
              ,case when (cg.hphone_extn = '' OR cg.hphone_extn is NULL) THEN '' ELSE concat(', ',cg.hphone_extn) END
              ),2)) as 'candidate-phone'	
       ,ltrim(substring(concat(
               case when (cg.wphone = '' OR cg.wphone is NULL) THEN '' ELSE concat(', ',cg.wphone) END
              ,case when (cg.wphone_extn = '' OR cg.wphone_extn is NULL) THEN '' ELSE concat(', ',cg.wphone_extn) END
              ),2)) as 'candidate-phone'
       ,edu.note as 'candidate-education'
       ,wh.note as 'candidate-workhistory'
       ,sk.note as 'candidate-skills'
       #,notes as 'ACTIVITIES COMMENTS'
       #,subject,ConversationTopic as 'ACTIVITIES COMMENTS'
       #,body, filecontent as 'FILES'
        #,doc_id as 'FILES'
        ,cl.ctime as 'candidate-RegDate'
#select * #select count(distinct sno) #98389# select cg.owner
from candidate_general cg
left join (select l.username, t.email from candidate_list l left join TRUONG_companyowneremail t on t.owner = l.owner where t.email is not null ) o on o.username = cg.username
left join countries on countries.sno = cg.country
left join (select sno, type, name from manage where type = 'candsourcetype' ) source on source.sno = cg.cg_sourcetype
left join (
       select e.username,
              group_concat(
               case when e.heducation in (null,'','-') THEN '' ELSE concat('School or Program Name: ',e.heducation,'. ') END
              ,case when e.edudegree_level in (null,'','-') THEN '' ELSE concat('Degree/Level Attained: ',e.edudegree_level,'. ') END
              ,case when e.edudate in (null,'','-') THEN '' ELSE concat('Completion Date: ',e.edudate,'. ') END
              ,case when e.educity in (null,'','-') THEN '' ELSE concat('City / Town: ',e.educity,'. ') END
              ,case when e.edustate in (null,'','-') THEN '' ELSE concat('State: ',e.edustate,'. ') END
              ,case when (countries.country is null or countries.country = '')  THEN '' ELSE concat('Country: ',countries.country,'. ') END
              SEPARATOR '\n') as 'note'
       #select * 
       from candidate_edu e
       left join countries on countries.sno = e.educountry
       #where e.educity <> '' or  e.edustate <> '' or  e.educountry <> '' #e.username in ('cand10002','cand48')#
       group by e.username
       ) edu on edu.username = cg.username
left join (
       select username,
              group_concat(
                      case when (cw.cname = ''  OR cw.cname is NULL) THEN '' ELSE concat('Employer Name: ',cw.cname,char(13)) END
                     ,case when (cw.ftitle = '' OR cw.ftitle is NULL) THEN '' ELSE concat('Job Title: ',cw.ftitle,char(13)) END
                     ,case when (cw.sdate = '' OR cw.sdate is NULL) THEN '' ELSE concat('Start Date: ',cw.sdate,char(13)) END
                     ,case when (cw.edate = '' OR cw.edate is NULL) THEN '' ELSE concat('End Date: ',cw.edate,char(13)) END
                     
                     ,case when (cw.city = '' OR cw.city is NULL) THEN '' ELSE concat('City: ',cw.city,char(13)) END
                     ,case when (cw.state = '' OR cw.state is NULL) THEN '' ELSE concat('State: ',cw.state,char(13)) END
                     ,case when (cw.country = '' OR cw.country is NULL) THEN '' ELSE concat('Country: ',cw.country,char(13)) END
                     ,case when (cw.compensation_beginning = '' OR cw.compensation_beginning is NULL) THEN '' ELSE concat('Compensation: ',cw.compensation_beginning,char(13)) END
                     ,case when (cw.leaving_reason = '' OR cw.leaving_reason is NULL) THEN '' ELSE concat('Reason for Leaving: ',cw.leaving_reason,char(13)) END
                     ,case when (cw.wdesc = '' OR cw.wdesc is NULL) THEN '' ELSE concat('Description: ',cw.wdesc,char(13)) END
              SEPARATOR '\n\n\n') as 'note'
       #select * #select count(*) #select 
       from truong_candidatewh cw
       #where username in ('cand48','cand60687') 
       group by cw.username
       order by username,rn desc
       ) wh on wh.username = cg.username
left join (
       SELECT cs.username,
                #,skillname as 'FUNCTIONAL EXPERTISE'
                group_concat(
                      case when (cs.skillname = '' OR cs.skillname = '0' OR cs.skillname is NULL) THEN '' ELSE concat('Skill Name: ',cs.skillname,char(13)) END
                     ,case when (cs.lastused = '' OR cs.lastused = '0' OR cs.lastused is NULL) THEN '' ELSE concat('Last Used: ',cs.lastused,char(13)) END
                     ,case when (cs.skilllevel = '' OR cs.skilllevel = '0' OR cs.skilllevel is NULL) THEN '' ELSE concat('Skill Level: ',cs.skilllevel,char(13)) END
                     ,case when (cs.skillyear = '' OR cs.skillyear = '0' OR cs.skillyear is NULL) THEN '' ELSE concat('Skill Year: ',cs.skillyear,char(13)) END
                     #,case when (s.skill_name is NULL or  s.skill_name = '') THEN '' ELSE concat('manage_skills_id: ',s.skill_name,char(13)) END ,cs.manage_skills_id 
              SEPARATOR '\n\n') as 'note'
       FROM candidate_skills cs 
       #left join manage_skills s on s.sno = cs.manage_skills_id
       #where username in ('cand11956','cand11958','cand9869') #  (cs.manage_skills_id <> '' and cs.manage_skills_id <> 0) cs.lastused <> '' or cs.skilllevel <> '' or cs.skillyear <> '' or
       group by cs.username
       ) sk on sk.username = cg.username
left join candidate_list cl on cl.username cg.username
#where cg.cg_sourcetype <> 0
where cg.username in ('cand34962','cand90','cand9869','cand11956','cand11957','cand11958','cand10002','cand48') #or (fname = 'Gabriela' and lname = 'Quinones')
group by cg.username


select * from candidate_general where username  = 'cand34962'
		
