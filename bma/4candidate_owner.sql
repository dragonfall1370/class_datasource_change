
SET group_concat_max_len = 2147483647;     
 
 
  
DROP TABLE IF EXISTS truong_candidateowner;
CREATE TABLE IF NOT EXISTS truong_candidateowner as
       select distinct l.username, t.email 
       from candidate_list l 
       left join TRUONG_companyowneremail t on t.owner = l.owner 
       where t.email is not null;
select count(*) from truong_candidateowner;
select * from truong_candidateowner;



DROP TABLE IF EXISTS truong_candidateowner_ordered;
CREATE TABLE IF NOT EXISTS truong_candidateowner_ordered as     
       SELECT 
                        @rn := case when @username = username then @rn + 1 else 1 end AS rn,
                        @username := username as username, email
       FROM truong_candidateowner
       #where username in ('cand84701','cand84702','cand84711','cand84713','cand84716','cand84724','cand84728','cand84729','cand84730','cand84731')
       ORDER BY username,email DESC;
       
select * from truong_candidateowner_ordered;
select count(distinct username) from truong_candidateowner #98845
#select username from truong_candidateowner  group by username having count(*) > 1
#select count(*) FROM truong_candidateowner_ordered where username = '' #572




