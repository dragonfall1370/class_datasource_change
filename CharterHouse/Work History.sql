with test as (select
trim(candcode) as candcode, startdate, 

--concat('Employer: ',employer,(char(13)+char(10)),'Start Date: ',startdate,(char(13)+char(10)),'Job Title: ',jobTitle) as Work_History, 
employer,jobtitle,
ROW_NUMBER() over ( partition by candcode order by startdate desc ) as rn
from candhistory)

--,test2 as (select candcode,startdate, Work_History  from test where rn <> 1)

--select candcode, string_agg(work_history,(char(13)+char(10))) as work_history from test2
--group by candcode


select candcode,(select employer as currentEmployer,jobtitle as jobTitle,1 as cb_Employer for json path) as json_detail from test where rn = 1
