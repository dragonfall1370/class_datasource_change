# NOTE: SET group_concat_max_len = 2147483647;

DROP TABLE IF EXISTS truong_candidateedu;
CREATE TABLE IF NOT EXISTS truong_candidateedu as
       select e.username,
              group_concat(
               case when e.heducation in (null,'','-') THEN '' ELSE concat('School or Program Name: ',e.heducation,'. ') END
              ,case when e.edudegree_level in (null,'','-') THEN '' ELSE concat('Degree/Level Attained: ',e.edudegree_level,'. ') END
              ,case when e.edudate in (null,'','-') THEN '' ELSE concat('Completion Date: ',e.edudate,'. ') END
              ,case when e.educity in (null,'','-') THEN '' ELSE concat('City / Town: ',e.educity,'. ') END
              ,case when e.edustate in (null,'','-') THEN '' ELSE concat('State: ',e.edustate,'. ') END
              ,case when (countries.country is null or countries.country = '')  THEN '' ELSE concat('Country: ',countries.country,'. ') END
              SEPARATOR '\n\n') as 'note'
       #select * 
       from candidate_edu e
       left join countries on countries.sno = e.educountry
       #where e.educity <> '' or  e.edustate <> '' or  e.educountry <> '' #e.username in ('cand10002','cand48')#
       group by e.username;
# select * from truong_candidateedu;


DROP TABLE IF EXISTS truong_candidateskill;
CREATE TABLE IF NOT EXISTS truong_candidateskill as
       SELECT cs.username,
                #,skillname as 'FUNCTIONAL EXPERTISE'
                group_concat(
                      #case when (cs.skillname = '' OR cs.skillname = '0' OR cs.skillname is NULL) THEN '' ELSE concat('Skill Name: ',cs.skillname,char(13)) END
                     #,case when (cs.lastused = '' OR cs.lastused = '0' OR cs.lastused is NULL) THEN '' ELSE concat('Last Used: ',cs.lastused,char(13)) END
                     #,case when (cs.skilllevel = '' OR cs.skilllevel = '0' OR cs.skilllevel is NULL) THEN '' ELSE concat('Skill Level: ',cs.skilllevel,char(13)) END
                     #,case when (cs.skillyear = '' OR cs.skillyear = '0' OR cs.skillyear is NULL) THEN '' ELSE concat('Skill Year: ',cs.skillyear,char(13)) END
                     ##,case when (s.skill_name is NULL or  s.skill_name = '') THEN '' ELSE concat('manage_skills_id: ',s.skill_name,char(13)) END ,cs.manage_skills_id 
                     replace(
                            concat(
                             case when (cs.skillname = '' OR cs.skillname = '0' OR cs.skillname is NULL) THEN '' ELSE concat('',cs.skillname) END
                            ,' ('
                            ,case when (cs.skillyear = '' OR cs.skillyear = '0' OR cs.skillyear is NULL) THEN '' ELSE concat(cs.skillyear,'yrs') END                     
                            ,case when (cs.lastused = '' OR cs.lastused = '0' OR cs.lastused is NULL) THEN '' ELSE concat(',', cs.lastused,' year ago') END
                            ,case when (cs.skilllevel = '' OR cs.skilllevel = '0' OR cs.skilllevel is NULL) THEN '' ELSE concat(',',cs.skilllevel) END
                            ,')' )
                     ,'()','')
              SEPARATOR '\n') as 'note'
       FROM candidate_skills cs
       where cs.username <> ''
       #left join manage_skills s on s.sno = cs.manage_skills_id
       #where username in ('cand11956','cand11958','cand9869') #  (cs.manage_skills_id <> '' and cs.manage_skills_id <> 0) cs.lastused <> '' or cs.skilllevel <> '' or cs.skillyear <> '' or
       group by cs.username;
# select * from truong_candidateskill;



