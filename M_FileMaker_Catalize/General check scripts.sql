--Company notes
select cmt.ContactServiceID, cmt.FullName, act.Subject, act.ActivityNote
from ContactMainTable cmt
left join ActivityContacts ac on cmt.ContactServiceID = ac.ContactID
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where cmt.Type = 2 and cmt.IsDeletedLocally = 0

--
select * from UserFieldDefinitions
order by FieldName

select * from UserFieldTableIds

select * from ContactCategoriesTable

select * from EntityUserFields euf
left join UserFieldDefinitions ufd on ufd.FieldGUID = euf.FieldGUID
where euf.EntityType = 1

select * from ContactAttachments

select * from Attachments

select * from ActivitiesTable
