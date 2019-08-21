
# NOTE:
SET group_concat_max_len = 2147483647;

select * from candidate_general where username  = 'cand34962'

-- OWNER
drop table TRUONG_companyowneremail;
create table TRUONG_companyowneremail as 
       select distinct c.owner ,users.name ,e.email
       from candidate_list c
       left join (select username, name from users) users on users.username = c.owner
       left join (select distinct ltrim(rtrim(name)) as name, email from emp_list where email is not null and email <> '' and email <> 'arletteassam@yahoo.com' and email <> 'jenniffercastroe@gmail.com') e on e.name = users.name;
select * from TRUONG_companyowneremail;

-- EDUCATION
       SELECT
                         @rn := case when @username = username then @rn + 1 else 1 end AS rn,
                         @username := username as username,
       heducation as 'Education > School or Program Name'
       ,edudegree_level as 'Education > Degree/Level Attained'
       ,edudate as 'Education > Completion Date'
from candidate_edu e
ORDER BY username, sno DESC



select
        cg.username as 'candidate-externalId'
       ,cg.profiletitle as 'Candidate-jobTitle1'
       ,case when (cg.fname = '' OR cg.fname is NULL) THEN 'No FirstName' ELSE cg.fname END as 'contact-firstName' #,cg.fname as 'candidate-firstName'
       ,cg.mname as 'candidate-MiddleName'
       ,case when (cg.lname = '' OR cg.lname is NULL) THEN 'No LastName' ELSE cg.lname END as 'contact-lastName' #,cg.lname as 'candidate-LastName'
       ,cg.alternate_email as 'candidate-PersonalEmail'
       ,case when ce.email is null then concat(cg.username,'@noemailaddress.co') else ce.email end as 'candidate-email' 
       #,group_concat(cg.email,cg.other_email SEPARATOR ',') as 'candidate-email'     
       #,ltrim(substring(concat(
       #         case when (cg.email = '' OR cg.email is NULL) THEN '' ELSE concat(', ',cg.email) END
       #         ,case when (cg.other_email = '' OR cg.other_email is NULL) THEN '' ELSE concat(', ',cg.other_email) END
       #         ,case when (cg.email = ''  and cg.other_email = '') THEN concat(cg.username,'@noemail.com ') END
       #         ),2)) as  'candidate-email'     
       ,source.name as 'candidate-Source' #cg.cg_sourcetype
       ,cg.mobile as 'candidate-mobile'
       ,o.email as 'candidate-owners'
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
               case when (cg.hphone is NULL or cg.hphone = '') THEN '' ELSE concat(', ',cg.hphone) END
              ,case when (cg.hphone_extn is NULL or cg.hphone_extn = '') THEN '' ELSE concat(', ',cg.hphone_extn) END
              ),2)) as 'candidate-phone'	
       ,ltrim(substring(concat(
               case when (cg.wphone is NULL or cg.wphone = '') THEN '' ELSE concat(', ',cg.wphone) END
              ,case when (cg.wphone_extn is NULL or cg.wphone_extn = '') THEN '' ELSE concat(', ',cg.wphone_extn) END
              ),2)) as 'candidate-workphone'
       ,edu.note as 'candidate-education'
       #,wh.note as 'candidate-workhistory' --<<
       ,sk.note as 'candidate-skills'
       ,n.note as 'candidate-note'
       ,doc.docs as 'candidate-resumes'
       
       #,notes as 'ACTIVITIES COMMENTS'
       #,subject,ConversationTopic as 'ACTIVITIES COMMENTS'
#select * # select count(*) #99999# select cg.owner
from candidate_general cg
left join (select * from truong_candidateowner_ordered where rn = 1) o on o.username = cg.username
left join countries on countries.sno = cg.country
left join (select sno, type, name from manage where type = 'candsourcetype' ) source on source.sno = cg.cg_sourcetype
left join truong_candidateedu edu on edu.username = cg.username
#left join truong_candidatewh_ordered wh on wh.username = cg.username --<<
left join truong_candidateskill sk on sk.username = cg.username
left join truong_candidateemail_ordered ce on ce.username = cg.username
left join truong_candidatenote n on n.username = cg.username
left join truong_candidatedoc doc on doc.username = cg.username
#where cg.username in ('cand34962','cand90','cand9869','cand11956','cand11957','cand11958','cand10002','cand48') #or (fname = 'Gabriela' and lname = 'Quinones')
#where cg.cg_sourcetype <> 0

desc candidate_general; create index candidate_general_index on candidate_general (username);
desc truong_candidateowner_ordered; create index truong_candidateowner_ordered_index on truong_candidateowner_ordered (username);
desc countries; create index countries_index on countries (sno);
desc manage; ; create index manage_index on manage (sno);
desc truong_candidateedu edu; create index truong_candidateedu_index on truong_candidateedu (username);
desc truong_candidatewh_ordered; create index truong_candidatewh_ordered_index on truong_candidatewh_ordered (username);
desc truong_candidateskill; create index truong_candidateskill_index on truong_candidateskill (username);
desc truong_candidateemail_ordered; create index truong_candidateemail_ordered_index on truong_candidateemail_ordered (username);
desc truong_candidatenote; create index truong_candidatenotel_index on truong_candidatenote (username);
desc truong_candidatedoc; create index truong_candidatedoc_index on truong_candidatedoc (username);

/* REG. DATE
select
        cg.username as 'candidate-externalId'
        #,cl.ctime as 'candidate-RegDate'
        ,timestamp(cl.ctime) as 'candidate-RegDate'
#select * # select count(*) #98389# select cg.owner
from candidate_general cg
left join candidate_list cl on cl.username = cg.username 
where cg.username  = 'cand34962'
*/