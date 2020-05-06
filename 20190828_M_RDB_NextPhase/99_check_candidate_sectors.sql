--CANDIDATE in both sectors: Main sector and Deleted candidates
with email as (select p.ObjectID, p.PhoneId
		, TRIM('.' FROM p.Num) as Email--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from Applicants a
		left join Phones p on p.PhoneId = a.PrimaryEmailAddressPhoneId
        --where p.CommunicationTypeId=78 --CommunicationTypes: Email --Email cannot be retrieved from PrimaryEmailAddressPhoneId
        and p.Num like '%_@_%.__%'
		)
, mail_transform as (select ObjectID, PhoneId
				, replace(replace(translate (Email, '!'':"<>[]();,+', '             '), char(10), ' '), char(13), ' ') as Email
				from email
		)
, mail_split as (select ObjectID, PhoneId
				, trim(value) as email
				from mail_transform
				cross apply string_split(Email, ' ')
				where Email like '%_@_%.__%'
			) --select * from mail_split where ObjectId = 52038
, dup as (select ObjectID --EMAIL CHECK DUPLICATION
		, trim(' ' from email) as EmailAddress
		, row_number() over(partition by trim(' ' from email) order by ObjectID asc) as rn --distinct email if emails exist more than once
		, row_number() over(partition by ObjectID order by PhoneId desc) as Contactrn --distinct if contacts may have more than 1 email
		from mail_split
		where Email like '%_@_%.__%'
		)
, primaryEmail as (select ObjectID --PRIMARY EMAIL
		, case when rn > 1 then concat(rn,'_',EmailAddress)
		else EmailAddress end as PrimaryEmail
		from dup
		where EmailAddress is not NULL and EmailAddress <> ''
		and Contactrn = 1
		)

, cand_2sector as (select ObjectId, count(sectorId) as counts
		from SectorObjects
		where SectorId in (47, 48)
		and objectid in (select ObjectID from Objects where ObjectTypeId = 1)
		group by ObjectId
		having count(sectorId) > 1) --49 rows

select p.PersonID
, p.PersonName
, p.Surname
, pe.PrimaryEmail
from Person p
left join primaryEmail pe on pe.ObjectID = p.PersonID
where p.PersonID in (select ObjectId from cand_2sector)