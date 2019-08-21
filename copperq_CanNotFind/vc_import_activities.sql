
select cc.canid,cc.condate,cc.condetail
        , u.udf1 as owner
        , cast(4 as SIGNED) as contact_method
        , cast(1 as SIGNED) as related_status
from CanContact cc 
left join emergingsearch.candidate ca ON ca.CanID = cc.canID 
left join emergingsearch.users u ON u.userID = cc.userid
where ca.canID is not null 
#and u.udf1 is not null 
#and cc.id = 2672
and cc.canid = 87809679
#27478 (400 null)

###################################
select * from user_account where id = 28987 or email = 'caleb@emergingsc.com'
select * from candidate where external_id::int = 81239697
insert into position_candidate_feedback(candidate_id,user_account_id,comment_body, feedback_timestamp, insert_timestamp, contact_method, related_status) values(194408, -10, 'ABC', '2015-10-16 12:07:10', '2015-10-16 12:07:10', 4, 1)
select * from position_candidate_feedback
######################################################################



select cc.cliid,cc.condate,cc.note
        , u.udf1 as owners,cl.cliname
from CliContact cc 
left join emergingsearch.client cl on cl.CliID = cc.cliID 
left join emergingsearch.users u ON u.userid = cc.userid
where cl.cliID is not null and cc.cliID = 11745148
#24183 (206 null)

###################################
select * from company where external_id::int = 11745237 or id = 9746
select * from company_comment where company_id = 9323
select * from contact where first_name like '%Portia%'
insert into company_comment(company_id,user_id,comment_content,json_relate_info,comment_timestamp) values (9746,28987,'abc','["10197"]','2017-04-13 10:38:43')
######################################################################
