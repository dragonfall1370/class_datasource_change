
-- TASK WITH EMAIL ATTACHMENT INCLUDED
with a as ( 
       select  t.id --,t.whoid, t.subject, t.description, a.id, a.parentid
              , concat(a.id,'_',replace(a.Name,',','') ) as doc
       -- select count(*) --114426
       from task t
       left join attachment a on a.parentid = t.id where a.Name <> '' )
select * into truong_task_att0 from a

with a1 as (select id, STUFF((SELECT ', ' + doc from truong_task_att0 WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs  from truong_task_att0 a GROUP BY a.id )
select * into truong_task_att from a1


update task
set truong_att = truong_task_att.docs
from truong_task_att 
where task.id = truong_task_att.id