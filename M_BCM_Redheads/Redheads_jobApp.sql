select concat('RH',ef.ContactServiceID) as 'application-positionExternalId'
, cmt.FullName, cmt.Type, cmt.Subject
, concat('RH',ef.LinkID) as 'application-candidateExternalId'
, 'SHORTLISTED' as 'application-stage'
, ef.LinkItemType, cmt2.FullName
from EntityReferences ef
left join ContactMainTable cmt on cmt.ContactServiceID = ef.ContactServiceID
left join ContactMainTable cmt2 on cmt2.ContactServiceID = ef.LinkID
where cmt.Type = 3
and ef.ContactServiceID in (select ContactServiceID from ContactMainTable where Type = 3 and IsDeletedLocally = 0) --> escape deleted jobs
and ef.LinkID in (select ContactServiceID from ContactMainTable where Type = 1 and IsDeletedLocally = 0) --> escape deleted contacts/candidates