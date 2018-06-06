--BUSINESS NOTE AND COMMUNICATION NOTE
create table ContactBusinessNote
(ContactID int PRIMARY KEY,
ContactType int,
ContactBusinessNote nvarchar(max)
)
go

with BusinessNote as (
select acc.ContactID, acc.ContactType, act.ActivityID, act.Subject, act.ActivityNote, act.CreatedOn, act.CreatedBy, act.MessageDeliveryTime, act.ModifiedOn
from ActivityContacts acc
left join ActivitiesTable act on act.ActivityID = acc.ActivityID
where act.ActivityType = 14 and act.ActivityNote is not NULL)

insert into ContactBusinessNote SELECT
     ContactID,
	 ContactType,
     STUFF(
         (SELECT '<hr>' + char(10) + 'Created on: ' + convert(varchar(16),CreatedOn,120) + char(10) + 'Created by: ' + right(CreatedBy,len(CreatedBy)-charindex('\',CreatedBy)) + char(10) + 'Modified on: ' + convert(varchar(16),CreatedOn,120) + char(10)
		  + 'Message Delivery Time: ' + convert(varchar(16),MessageDeliveryTime,120) + char(10) + 'Subject: ' + Subject + char(10) 
		  + REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( cast(ActivityNote as nvarchar(max))
			,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
			,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
			,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
			,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
			,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
			,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
          from  BusinessNote
          WHERE ContactID = a.ContactID
		  order by ModifiedOn desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 4, '')  AS ContactBusinessNote
FROM BusinessNote as a
GROUP BY a.ContactID, a.ContactType

select * from ContactBusinessNote;

--COMMUNICATION NOTE
create table ContactCommunicationNote
(ContactID int PRIMARY KEY,
ContactType int,
ContactCommunicationNote nvarchar(max)
)
go


with CommunicationNote as (
select acc.ContactID, acc.ContactType, act.ActivityID, act.Subject, act.ActivityNote, act.CreatedOn, act.CreatedBy, act.MessageDeliveryTime, act.ModifiedOn
from ActivityContacts acc
left join ActivitiesTable act on act.ActivityID = acc.ActivityID
where act.ActivityType in (15,3) and act.ActivityNote is not NULL)

insert into ContactCommunicationNote SELECT
     ContactID, ContactType,
     STUFF(
         (SELECT '<hr>' + char(10) + 'Created on: ' + convert(varchar(16),CreatedOn,120) + char(10) + 'Created by: ' + right(CreatedBy,len(CreatedBy)-charindex('\',CreatedBy)) + char(10) + 'Modified on: ' + convert(varchar(16),CreatedOn,120) + char(10)
		  + 'Message Delivery Time: ' + convert(varchar(16),MessageDeliveryTime,120) + char(10) + 'Subject: ' + Subject + char(10) 
		  + REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
			REPLACE( REPLACE( REPLACE( REPLACE( cast(ActivityNote as nvarchar(max))
			,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
			,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
			,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
			,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
			,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
			,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
		from  CommunicationNote
        WHERE ContactID = a.ContactID
		order by ModifiedOn desc
        FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
        , 1, 4, '')  AS ContactCommunicationNote
FROM CommunicationNote as a
GROUP BY a.ContactID, ContactType

select * from ContactCommunicationNote