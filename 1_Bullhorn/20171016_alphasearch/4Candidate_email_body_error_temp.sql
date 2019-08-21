/*
select * from position_candidate_feedback_t limit 100

select * from position_candidate_feedback --_t 27/09/2017 04:21:27
limit 10

update position_candidate_feedback_t 
set userid = candidate.id
from candidate
where candidate.external_id::int = position_candidate_feedback_t.candidate_id

insert into position_candidate_feedback (candidate_id,user_account_id,contact_method,related_status,insert_timestamp,feedback_timestamp,comment_body)
select 
userid
,user_account_id
,contact_method
,related_status
,feedback_timestamp_insert_timestamp::timestamp --07/01/2012 12:14:37
,feedback_timestamp_insert_timestamp::timestamp
,comment_body
 --select count(*) 
 from position_candidate_feedback_t where userid is not null 
 limit 100
*/
-- select name from bullhorn1.bh_usermessagefile where name LIKE '%' + CHAR(0) + '%';


      SELECT --top 6000
                     userMessageID,
                     STUFF(
                         (SELECT  ' , ' 
                                --+ cast(name as varchar(64)) 
                                --+ cast(REPLACE(name,char(0),'') as varchar(64))
                                + Replace(name, Ascii(0x0C), '')
                          from  bullhorn1.BH_UserMessageFile
                          WHERE userMessageID = a.userMessageID
                          --FOR XML PATH (''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '')  AS att
                          --FOR XML PATH(''), TYPE).value('(./text())[1]', 'VARCHAR(max)'), 1, 2, '') AS att
                          FOR XML path('')), 1, 2, '') AS att
                FROM bullhorn1.BH_UserMessageFile as a
                GROUP BY a.userMessageID        
                