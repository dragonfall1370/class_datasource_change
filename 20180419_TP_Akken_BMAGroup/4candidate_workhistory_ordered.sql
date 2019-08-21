# NOTE: SET group_concat_max_len = 2147483647;


-- WORK HISTORY     
       
/*
alter table candidate_work add column sdatefrom VARCHAR(15);
alter table candidate_work add column sdateto VARCHAR(15);
alter table candidate_work add column edatefrom VARCHAR(15);
alter table candidate_work add column edateto VARCHAR(15);
alter table candidate_work add column rn VARCHAR(15);

SELECT SPLIT_STR(cw.sdate, '-', 1) as sdatefrom,
              SPLIT_STR(cw.sdate, '-', 2) as sdateto,
              SPLIT_STR(cw.edate, '-', 1) as edatefrom,
              SPLIT_STR(cw.edate, '-', 2) as edateto
from candidate_work cw

update candidate_work set sdatefrom = SPLIT_STR(sdate, '-', 1);
update candidate_work set sdateto = SPLIT_STR(sdate, '-', 2);
update candidate_work set edatefrom = SPLIT_STR(edate, '-', 1);
update candidate_work set edateto = SPLIT_STR(edate, '-', 2);
update candidate_work set edateto = '3000' where edateto in ('','0') and edatefrom = 'Present';
*/


#SELECT 0 INTO @rn;
#SET @rn = 0;        
#select count(*) FROM candidate_work where username = '' #572

        
DROP TABLE IF EXISTS truong_candidatewh;
CREATE TABLE IF NOT EXISTS truong_candidatewh as
       SELECT 
                         @rn := case when @username = username then @rn + 1 else 1 end AS rn,
                         @username := username as username,
                         sno,cname,ftitle,sdate,edate,city,state,country,compensation_beginning,leaving_reason,wdesc,sdatefrom,sdateto,edatefrom,edateto                 
       FROM candidate_work
       where username <> '' #and username in ('cand29013','cand60687','cand73182','cand82585') 
       ORDER BY username ,edateto DESC, sdateto DESC, sno DESC;
# select * from truong_candidatewh;


DROP TABLE IF EXISTS truong_candidatewh_ordered;
CREATE TABLE IF NOT EXISTS truong_candidatewh_ordered as
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
       order by username,rn desc;


select count(*) from truong_candidatewh_ordered where note is not null;
select * from truong_candidatewh_ordered;
select * from candidate_general where sno = 10002;