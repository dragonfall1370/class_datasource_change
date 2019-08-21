
SET group_concat_max_len = 2147483647;



DROP TABLE IF EXISTS truong_candidateemail;
CREATE TABLE IF NOT EXISTS truong_candidateemail as
       SELECT 
                         @rn := case when @email = t.email then @rn + 1 else 1 end AS rn,
                         @email := t.email as email,
                         t.username
       FROM ( select username,ltrim(rtrim(replace(replace(email,CHAR(9),''),'''','') )) as email from candidate_general where email <> ''
                    union all 
                    select username,ltrim(rtrim(replace(replace(other_email,CHAR(9),''),'''','') )) as email from candidate_general where other_email <> '' ) t
       #where t.username in ('cand58227','cand58228','cand22779','cand22781')
       ORDER BY t.email ,t.username asc;
select * from truong_candidateemail where email in ('a.munoz79@hotmail.com');
select email from truong_candidateemail group by email having count(*) > 1;
# select count(*) FROM truong_candidateemail where username = '' #572



DROP TABLE IF EXISTS truong_candidateemail_ordered;
CREATE TABLE IF NOT EXISTS truong_candidateemail_ordered as
        select
                 username
                ,group_concat(
                        case when rn = 1 then email else concat(email,'_',rn) end 
                 separator ',' ) as  'email'
        from truong_candidateemail
        #where username in ('cand58227','cand58228','cand22779','cand22781')
        group by username;
select * from truong_candidateemail_ordered;
select count(*) FROM truong_candidateemail_ordered where  email in ('a.munoz79@hotmail.com') #or username
select * FROM truong_candidateemail_ordered where  email like '%a.munoz79@hotmail.com%'