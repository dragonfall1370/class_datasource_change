
with
 tag as (SELECT app_id,STUFF((SELECT char(10)  + 'Date: ' + convert(varchar(10), actdate, 120) + ' ' + convert(varchar(10), acttime, 120) 
	+ char(10) + 'Author: ' + case when ( c.first_name is null) then '(no author)' else concat(c.first_name,' ',c.last_name) end + char(10) 
	+ '- ' + b.act_name + ': ' + cast(actnotes as varchar(max)) + char(10)
	from app_activity 
	left join app_activity_type b on acttype = b.id  
	left join users c on act_auth = c.ID
	WHERE app_id = a.app_id order by actdate FOR XML PATH ('')), 1, 1, '')  AS tag 
FROM  app_activity AS a GROUP BY a.app_id)
--select * from tag where app_id = 8428418

, comment as (
	select ap.id as 'candidate-externalId'
	, ap.first_name as 'candidate-firstName'
	, ap.last_name as 'candidate-Lastname'
	, iif(ap.email = '' or ap.email is NULL,concat(ap.id,'_candidate@noemail.com'),ap.email) as 'candidate-email'
	,replace(replace(replace(replace(concat(case when (tag.tag is null) then '' else concat('Activities: ',char(10),tag.tag,char(10)) end,case when (ap.comments is null) then '' else concat('Comments: ',char(10),ap.comments,char(10)) end),'&lt;br /&gt;','.'),'&amp;',''),'nbsp;',''),'#39;','') as 'candidate-comments'
	from applicants ap
	left join tag on ap.id = tag.app_id)

select top 10 *, LEN([candidate-comments]) as LENGTH from comment
order by LENGTH desc

