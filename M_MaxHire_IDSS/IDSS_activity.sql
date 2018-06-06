--CONTACT ACTIVITY
select concat('IDSS',contacts_id) as IDSS_ContactExtID
	, -10 as Sirius_user_account_id
	, Activityfirstname
	, Activitylastname
	, destination
	, concat(coalesce('Created date: ' + nullif(convert(varchar(20),dateenter,121),'') + char(10),'')
		, coalesce('Regarding: ' + nullif(regarding,'') + char(10),'')
		, coalesce('Activity media: ' + nullif(activitymedia,'') + char(10),'')
		, coalesce('Activity counsel: ' + nullif(activitycounsel,'') + char(10),'')
		, coalesce('Activity name: ' + nullif(coalesce(Activityfirstname + ' ' + Activitylastname,''),'') + char(10),'')
		, coalesce('Destination: ' + nullif(destination,'') + char(10),'')
		, coalesce('Activity notes: ' + nullif(convert(nvarchar(max),activitynotes),''),'')
		) as IDSS_comments
	, convert(datetime, dateenter, 121) as IDSS_insert_timestamp
	, 'comment' as IDSS_category
	, 'contact' as IDSS_type
from activity
where exists(select cid from people where activity.contacts_id = people.cid and RoleType = 1 and DeleteFlag = 0) --example: 248 | 75 rows


--CANDIDATE ACTIVITY
select concat('IDSS',cid) as IDSS_CandidateExtID
	, -10 as Sirius_user_account_id
	, Activityfirstname
	, Activitylastname
	, destination
	, concat(coalesce('Created date: ' + nullif(convert(varchar(20),dateenter,121),'') + char(10),'')
		, coalesce('Regarding: ' + nullif(regarding,'') + char(10),'')
		, coalesce('Activity media: ' + nullif(activitymedia,'') + char(10),'')
		, coalesce('Activity counsel: ' + nullif(activitycounsel,'') + char(10),'')
		, coalesce('Activity name: ' + nullif(coalesce(Activityfirstname + ' ' + Activitylastname,''),'') + char(10),'')
		, coalesce('Destination: ' + nullif(destination,'') + char(10),'')
		, coalesce('Activity notes: ' + nullif(convert(nvarchar(max),activitynotes),''),'')
		) as IDSS_comments
	, convert(datetime, dateenter, 121) as IDSS_insert_timestamp
	, 'comment' as IDSS_category
	, 'candidate' as IDSS_type
from activity
where exists(select cid from people where activity.cid = people.cid and RoleType = 0 and DeleteFlag = 0)