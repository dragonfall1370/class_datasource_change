--Update job title by removing (number)
select pd.id
, pd.name
, m.job_title
from position_description pd
join mike_tmp_pa_job m on pd.id = m.job_id
where position(' - ' in pd.name) > 1 --30130


--UPDATE JOB TITLE BY REMOVING NUMBER
update position_description pd
set name = m.job_title
from mike_tmp_pa_job m
where 1=1
and pd.id = m.job_id
and position(' - ' in pd.name) > 1


/* USING THIS TO REMOVE THE NUMBER INDEX
SELECT 'アプリケーションエンジニア - 33',
       substring('アプリケーションエンジニア - 33' from '^.*?(?=[0-9]|$)') AS street,
       substring('アプリケーションエンジニア - 33' from '[0-9].*$') AS housenum
*/