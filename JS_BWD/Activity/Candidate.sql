with candidate as (select * from people)

select  a.peo_no as 'external_id',
cast(replace(replace(log_start_date,left(log_start_date,4),concat(left(log_start_date,4),'-')),left(replace(log_start_date,left(log_start_date,4),concat(left(log_start_date,4),'-')),7),
concat(left(replace(log_start_date,left(log_start_date,4),concat(left(log_start_date,4),'-')),7),'-')) as datetime) as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id',
concat(
--'Name: ', b.last_name, ' ' , b.first_name, (char(13)+char(10)),
nullif(concat('Author: ',b.peo_forename,' ',b.peo_surname,' - ',b.peo_cnt_email,(char(13)+char(10))),concat('Author: ',' ',' - ',(char(13)+char(10)))),
nullif(concat('To: ',c.peo_forename,' ',c.peo_surname,' - ',c.peo_email,(char(13)+char(10))),concat('To: ',' ',' - ',(char(13)+char(10)))),
nullif(concat('Subject: ',log_action,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',log_subject),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content'


from c_log a
left join people b on a.cnt_peo_no = b.peo_no
left join candidate c on a.peo_no = c.peo_no
where a.peo_no <> 0
order by a.peo_no



