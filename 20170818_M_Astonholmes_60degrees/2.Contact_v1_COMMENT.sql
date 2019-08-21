
With
 tmp5 as (select
a.mgr_id
, b.user_email
, concat(b.first_name,b.last_name) as ownerName
from (select mgr_id, max(user_id) as user_id from mgr_rep group by mgr_id) a
left join users b on a.user_id = b.ID)

,comment as (select 
hm.comp_id as 'contact-companyId'
, hm.branch_id
, hm.id as 'contact-externalId'
, hm.first_name as 'contact-firstName'
, hm.last_name as 'contact-lastName'
, tmp5.user_email as 'contact-owners'
, replace(ca.comments,'&#x0D;','') as 'contact-comments'
from hiring_manager hm
left join tmp5 on hm.id = tmp5.mgr_id
left join (SELECT mgr_id,STUFF((SELECT DISTINCT ',' + convert(varchar(max), actnotes) from cont_activity WHERE mgr_id = a.mgr_id FOR XML PATH ('')), 1, 1, '')  AS comments FROM cont_activity AS a GROUP BY a.mgr_id) ca on hm.id = ca.mgr_id
)

select top 10 *, LEN([contact-comments]) as LENGTH from comment
order by LENGTH desc
