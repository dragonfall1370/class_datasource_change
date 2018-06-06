--ORIGINAL CONTACTS COMMENTS
select concat('CA',acc.ContactID) as CandidateExtID
, acc.ContactType
, act.ActivityID
, act.Subject
, act.ActivityNote
, act.CreatedOn
, act.CreatedBy
, act.MessageDeliveryTime
, act.ModifiedOn
from ActivityContacts acc
left join ActivitiesTable act on act.ActivityID = acc.ActivityID
where act.ActivityType in (3,14) and act.ActivityNote is not NULL


--EDITED CONTACTS ACTIVITIES | 6155 rows
select concat('CA',acc.ContactID) as CA_ContactExtID
, -10 as CA_user_account_id
, concat('Created by: ', act.CreatedBy, char(10)
	, 'Subject: ', act.Subject, char(10)
	, 'ActivityNote: ', char(10), act.ActivityNote) as CA_ContactComments
, act.MessageDeliveryTime as CA_insert_timestamp
, 'comment' as CA_category
, 'contact' as CA_type
, acc.ContactType
, act.ActivityID
from ActivityContacts acc
left join ActivitiesTable act on act.ActivityID = acc.ActivityID
where act.ActivityType in (3,14) and act.ActivityNote is not NULL
and acc.ContactID in (select ContactServiceID from ContactCategoriesTable where CategoryName in ('Non-finance') group by ContactServiceID)



--CANDIDATE ACTIVITIES
select concat('CA',acc.ContactID) as CA_CandidateExtID
, -10 as CA_user_account_id
, concat('Created by: ', act.CreatedBy, char(10)
	, 'Subject: ', act.Subject, char(10)
	, 'ActivityNote: ', char(10), act.ActivityNote) as CA_CandidateComments
, act.MessageDeliveryTime as CA_insert_timestamp
, 'comment' as CA_category
, 'candidate' as CA_type
from ActivityContacts acc
left join ActivitiesTable act on act.ActivityID = acc.ActivityID
where act.ActivityType in (3,14) and act.ActivityNote is not NULL
and acc.ContactID not in (select ContactServiceID from ContactCategoriesTable where CategoryName in ('Non-finance') group by ContactServiceID)

UNION ALL

select
concat('CA',cmt.ContactServiceID) as CA_CandidateExtID
, -10 as CA_user_account_id
, cdt.ContactNotes as CA_CandidateComments
, getdate() as CA_insert_timestamp
, 'comment' as CA_category
, 'candidate' as CA_type
from ContactMainTable cmt
left join (select ContactServiceID, max(CategoryName) as CategoryName from ContactCategoriesTable
	where CategoryName in ('Non-finance')
	group by ContactServiceID) cct on cct.ContactServiceID = cmt.ContactServiceID -- conditions to filter Contact/Candidate based on mapping | 1 contact may have multiple statuses
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID --> candidate additional info
where cmt.Type = 1 and cmt.IsDeletedLocally = 0 and cdt.ContactNotes is not NULL and ltrim(cast(cdt.ContactNotes as nvarchar(max))) <> '' 
and replace(replace(cast(cdt.ContactNotes as nvarchar(max)),char(10),''),char(13),'') <> ''
and cmt.ContactServiceID not in (select ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Non-finance')
	group by ContactServiceID) --> conditional mapping for contact/candidate type | to exclude contacts
	
Algemeen -> Notities:   
----------------
--COMPANY ACTIVITIES (updated from 23/01/2018)
----------------
select concat('CA',acc.ContactID) as CA_CompExtID
, -10 as CA_user_account_id
, concat('Created by: ', act.CreatedBy, char(10)
	, 'Subject: ', act.Subject, char(10)
	, 'ActivityNote: ', char(10), act.ActivityNote) as CA_CompComments
, act.MessageDeliveryTime as CA_insert_timestamp
, 'comment' as CA_category
, 'company' as CA_type
, acc.ContactType
, act.ActivityID 
from ActivityContacts acc
left join ActivitiesTable act on act.ActivityID = acc.ActivityID
where act.ActivityType in (3,14) and act.ActivityNote is not NULL
and acc.ContactID in (select ContactServiceID from ContactMainTable where Type = 2 and IsDeletedLocally = 0)

--COMPANY NOTES in ContactDetailsTable
select concat('CA',cmt.ContactServiceID) as CA_CompExtID
, -10 as CA_user_account_id
, concat('Algemeen -> Notities: ', cast(cdt.ContactNotes as nvarchar(max))) as CA_CompComments
, getdate()- 14 as CA_insert_timestamp
, 'comment' as CA_category
, 'company' as CA_type
from ContactMainTable cmt
left join ContactDetailsTable cdt on cmt.ContactServiceID = cdt.ContactServiceID
where cmt.Type = 2 and cmt.IsDeletedLocally = 0
and cdt.ContactNotes is not NULL and ltrim(cast(cdt.ContactNotes as nvarchar(max))) <> '' and cast(cdt.ContactNotes as nvarchar(max)) <> '  '

----------------
--CONTACT ACTIVITIES (updated from 23/01/2018)
----------------
--CONTACT NOTES in ContactDetailsTable
select concat('CA',cmt.ContactServiceID) as CA_ContactExtID
, -10 as CA_user_account_id
, concat('Algemeen -> Notities: ', cast(cdt.ContactNotes as nvarchar(max))) as CA_ContactComments
, getdate()- 14 as CA_insert_timestamp
, 'comment' as CA_category
, 'contact' as CA_type
from ContactMainTable cmt
left join ContactDetailsTable cdt on cmt.ContactServiceID = cdt.ContactServiceID
where cmt.Type = 1 and cmt.IsDeletedLocally = 0
and cdt.ContactNotes is not NULL and ltrim(cast(cdt.ContactNotes as nvarchar(max))) <> '' and cast(cdt.ContactNotes as nvarchar(max)) <> '  '
and cmt.ContactServiceID in (select ContactServiceID from ContactCategoriesTable where CategoryName in ('Non-finance') group by ContactServiceID)