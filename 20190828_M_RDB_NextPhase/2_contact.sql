with contactinIDobject as (
        select o.ObjectID,o.LocationId,o.FileAs,o.FlagText,o.SourceId,o.CreatedOn
        from dbo.Objects o
        where o.ObjectTypeId=3 --contact
)
, alladdress as (
        select ad.ObjectId,ad.AddressId,adt.Description,ad.Building,ad.Street,ad.District,ad.City,ad.PostCode
		, lvct.ValueName as 'County'
		, lvc.ValueName as 'Country'
		, lvc.SystemCode as CountryCode
		, row_number() OVER(partition by ad.ObjectID order by ad.AddressID desc) AS rn
        from dbo.Address ad
        left join dbo.AddressTypes adt on ad.AddressTypeId=adt.AddressTypeId
        left join dbo.ListValues lvc on lvc.ListValueId=ad.CountryValueId --country
        left join dbo.ListValues lvct on lvct.ListValueId=ad.CountyValueId --county
) --select distinct Country from alladdress
, contact_address as (--All addresses group by contact
        select ObjectId
		, string_agg(concat_ws(', '
				, nullif(trim(Building),'')
				, nullif(trim(Street),'')
				, nullif(trim(District),'')
				, nullif(trim(City),'')
				, nullif(trim(PostCode),'')
				, nullif(trim(County),'')
				, nullif(trim(Country),'')), char(10)) as contact_address
        from alladdress
        group by ObjectId
)
, mobilephone as (--CONTACT MOBILE --INJECTED LATER
		select p.ObjectID,p.PhoneId,p.CreatedOn
		, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
			else trim(Num) end as phone
        from dbo.Phones p
        where p.CommunicationTypeId = 83 --Mobile
		) --reused for other entities
, mobile as (select ObjectID
		, string_agg(nullif(phone,''),',') as mobile
		from mobilephone
		group by ObjectID
		)
, primaryphone as (
		select p.ObjectID,p.PhoneId,p.CreatedOn
				, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
				else trim(Num) end as phone
				, row_number() OVER(partition by p.ObjectID order by p.PhoneId DESC) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId in (79) --phone as primary phone
		)
, contact_primaryphone as (select ObjectID,Phone as contact_primaryphone
        from primaryphone
        where rn=1
		)
, otherphone as (--ALL OTHER PHONES
				select p.ObjectID, p.PhoneId, p.CreatedOn, p.CommunicationTypeId
				, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
				else trim(Num) end as phone
        from Phones p
        where p.CommunicationTypeId in (81, 82, 87, 88) --phone: day, evening, office, home
		) 
, otherphone_group as (select ObjectID, CommunicationTypeId
				, string_agg(nullif(phone, ''),', ') as otherphone
				from otherphone
				group by ObjectID, CommunicationTypeId
		)
, allphone as (select ObjectID
		, string_agg(concat_ws(' '
			, case when CommunicationTypeId = 81 then 'Phone (Day):'
					when CommunicationTypeId = 82 then 'Phone (Evening):'
					when CommunicationTypeId = 87 then 'Phone (Home):'
					when CommunicationTypeId = 88 then 'Phone (Office):'
					end
			, nullif(otherphone,'')
			), char(10)) as allphone
		from otherphone_group
		group by ObjectID
		)
, fax as (--FAX
			select p.ObjectID,p.PhoneId,p.CreatedOn
				, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
				else trim(Num) end as fax
        from dbo.Phones p
        where p.CommunicationTypeId = 80 --fax
		)
, fax_contact as (select ObjectID
		, string_agg(nullif(fax,''),',') as fax_contact
		from fax
		group by ObjectID
		)
/* USING THIS IF PrimaryEmailAddressPhoneId NOT REFLECT EMAIL ADDRESS
, email as (--EMAIL
        select p.ObjectID, p.PhoneId
		, TRIM('.' FROM p.Num) as Email--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from Phones p
        where p.CommunicationTypeId=78 --CommunicationTypes: Email (78) --Email can be retrieved from PrimaryEmailAddressPhoneId
        and p.Num like '%_@_%.__%'
		)
*/
, email as (--EMAIL
        select cc.ClientContactId, p.ObjectID, p.PhoneId
		, TRIM('.' FROM p.Num) as Email--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from ClientContacts cc
		left join Phones p on p.PhoneId = cc.PrimaryEmailAddressPhoneId
        --where p.CommunicationTypeId=78 --CommunicationTypes: Email (78) --Email can be retrieved from PrimaryEmailAddressPhoneId
        and p.Num like '%_@_%.__%'
		)
, mail_transform as (select ClientContactId, PhoneId
				, replace(translate (Email, '!'':"<>[]();,+', '             '), char(10), ' ') as Email
				from email
		)
, mail_split as (select ClientContactId, PhoneId
				, trim(value) as email
				from mail_transform
				cross apply string_split(Email, ' ')
				where Email like '%_@_%.__%'
			) --select * from mail_split where ObjectId = 52038
, dup as (select ClientContactId --EMAIL CHECK DUPLICATION
		, trim(' ' from email) as EmailAddress
		, row_number() over(partition by trim(' ' from email) order by ClientContactId asc) as rn --distinct email if emails exist more than once
		, row_number() over(partition by ClientContactId order by PhoneId desc) as Contactrn --distinct if contacts may have more than 1 email
		from mail_split
		where Email like '%_@_%.__%'
		)
, primaryEmail as (select ClientContactId --PRIMARY EMAIL
		, case when rn > 1 then concat(rn,'_',EmailAddress)
		else EmailAddress end as PrimaryEmail
		from dup
		where EmailAddress is not NULL and EmailAddress <> ''
		and Contactrn = 1
		)
, officeemail as (--OFFICE EMAIL
        select p.ObjectID, STRING_AGG(TRIM('.' FROM p.Num),',') as officeemail--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId=85 --Email (Office)
        and p.Num like '%_@_%.__%'
        group by p.ObjectID
)
, personemail as (--PERSON EMAIL
        select p.ObjectID, STRING_AGG(TRIM('.' FROM p.Num),',') as personemail--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId=86 --Email (Personal)
        and p.Num like '%_@_%.__%'
        group by p.ObjectID
)
, network as (--NETWORK
        select p.ObjectID, STRING_AGG(TRIM('.' FROM p.Num),',') as network --,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from dbo.Phones p
        where p.CommunicationTypeId=90 --CommunicationTypes: Social Networking
		group by p.ObjectID
	)		
, consultant as (select cc.ContactConsultantId
				, cc.ClientContactId
				, cc.userId
				, u.EmailAddress
				, u.UserFullName
				, cc.UserRelationshipId
				, ur.Description
				, cc.CommissionPerc
				, g.GroupName
				from ContactConsultants cc
				left join Users u on cc.UserId=u.UserId
				left join UserRelationships ur on ur.UserRelationshipId = cc.UserRelationshipId
				left join Groups g on g.GroupId = cc.UserGroupId
)
, owners as (select ClientContactId
				, string_agg(EmailAddress, ',') within group (order by UserRelationshipId) as owners
				from consultant
				group by ClientContactId
			)
, consultant_info as (
        select ClientContactId
		, string_agg(
			concat_ws(' - '
				, nullif(UserFullName,'')
				, coalesce('Relationship: ' + nullif(Description, ''), NULL)
				, coalesce('Group name: ' + nullif(GroupName, ''), NULL)
				, coalesce('[Commission(%): ' + convert(varchar(max),CommissionPerc, 1)+ ']',NULL))
			, ', ') as consultant_info
        from consultant
        group by ClientContactId
)
, userinfo as (
        select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
        from Users u
)
, canvassperiod as (select c.ClientContactID,lv.ValueName as canvassperiod
        from dbo.ClientContacts c 
        left join dbo.ListValues lv  on c.CanvassPeriodValueId=lv.ListValueId
)
, person_info as (select p.PersonID, p.PersonName, p.Surname, p.Dob, p.Notes
                , p.GenderValueId
                , case when lv1.ValueName='Female' then 'FEMALE'
					when lv1.ValueName='Male' then 'MALE'  
					else null end as Gender
                , p.MaritalStatusValueId
				, lv2.ValueName as Marital
                , p.TitleValueId
				--, lv3.ValueName as Title
				, case 
                when lv3.ValueName = 'Dame' then 'MISS'
                when lv3.ValueName = 'Dr' then 'DR'
                when lv3.ValueName = 'Miss' then 'MISS'
                when lv3.ValueName = 'Mr' then 'MR'
                when lv3.ValueName = 'Mrs' then 'MRS'
                when lv3.ValueName = 'Ms' then 'MISS'
                when lv3.ValueName = 'Sir' then 'MR'
				end as Title
                , p.Salutation
				, p.NationalityId
				, n.Nationality
        from Person p
        left join ListValues lv1 on p.GenderValueId=lv1.ListValueId
        left join ListValues lv2 on p.MaritalStatusValueId=lv2.ListValueId
        left join ListValues lv3 on p.TitleValueId=lv3.ListValueId
        left join dbo.Nationality n on n.NationalityId=p.NationalityId
)
, contact_doc as (select cc.ClientContactId
		--, cc.ContactPersonId
		--, d.Description
		--, dc.FileExtension
		, string_agg(concat_ws('_','NP_D', d.DocumentID, concat(aa.ApplicantId, dc.FileExtension)),',') 
			within group (order by d.DocumentID desc) as candidate_doc
		from ClientContacts cc
		left join NotebookLinks nl on nl.ObjectId = cc.ContactPersonId
		left join Documents d on d.NotebookItemId = nl.NotebookItemId
		left join DocumentContent dc on dc.DocumentId = d.DocumentID
		where d.Description is not NULL
		and d.Description <> ''
		and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.msg','.txt','.htm','.html')
		group by cc.ClientContactId
		--total: 113871
		)
--MAIN SCRIPT
select concat('NP', cc.ClientContactId) as [contact-externalId]
        , case when cc.HasLeft = 'Y' then 'NP999999999'
			else concat('NP', cc.ClientID) end as [contact-companyId]
		, c.Company
		, coalesce(nullif(trim('\-?. ' FROM pi.PersonName),''),'No Firstname') as [contact-firstName]
        , coalesce(nullif(trim('\-?. ' FROM pi.Surname),''),'No Lastname') as [contact-lastName]
        , cc.JobTitle as [contact-jobTitle]
        , pi.Title as [contact-title]
		, pre.PrimaryEmail as [contact-email]
		, m.mobile --#inject later
		, cpp.contact_primaryphone as [contact-phone]
		, o.owners as [contact-owners]
		, concat_ws(char(10)
                , coalesce('External ID: ' + convert(varchar(max), cc.ClientContactId),NULL)
				, coalesce('Company: ' + case when cc.HasLeft = 'Y' then concat(c.Company, ' (left)') else c.Company end,NULL)
				, coalesce('Created by: ' + ui1.UserFullName,NULL)
                , coalesce('Created on: ' + convert(nvarchar(10),cc.CreatedOn,120),NULL)
                , coalesce('Gender: ' + nullif(pi.Gender,''),NULL)
                , coalesce('Salutation: ' + nullif(pi.Salutation,''),NULL)
				, coalesce(char(10) + '--Contact address--' + char(10) + ca.contact_address + char(10),NULL) --concatenate multiple addresses
                , coalesce('Office EMail: ' + nullif(oe.officeemail,''),NULL)
                , coalesce('Personal Email: ' + nullif(pe.personemail,''),NULL) --#Inject later
				, coalesce('--Other Phones--' + char(10) + nullif(ap.allphone,''),NULL)
                --, coalesce('Phone Day: ' + pd.Phone,NULL)
                --, coalesce('Phone Evening: ' + pn.Phone,NULL)
                --, coalesce('Home Phone: ' + hp.Phone,NULL)
                --, coalesce('Office Phone: ' + pol.officephonelist,NULL)
                , coalesce('Social Networking: ' + nullif(n.network,''),NULL)
                , coalesce('--Consultants Info--' + char(10) + nullif(ci.consultant_info,''),NULL)
				, coalesce(char(10) + '--Notes--' + char(10) + coalesce(nullif(pi.Notes,''),'NONE'), NULL)
        ) as [contact-Note]
from ClientContacts cc
left join primaryEmail pre on pre.ClientContactId = cc.ClientContactId --primary email only
left join contact_primaryphone cpp on cpp.ObjectID = cc.ContactPersonId --primary phone only
left join allphone ap on ap.ObjectID = cc.ContactPersonId --all other phones
left join mobile m on m.ObjectId = cc.ContactPersonId --mobile only
left join owners o on o.ClientContactId = cc.ClientContactId
left join person_info pi on pi.PersonID = cc.ContactPersonId
left join consultant_info ci on ci.ClientContactId = cc.ClientContactId
left join userinfo ui1 on ui1.UserId = cc.CreatedUserId
left join network n on n.ObjectID = cc.ContactPersonId
left join contact_address ca on ca.ObjectId = cc.ContactPersonId
left join canvassperiod cvp on cvp.ClientContactID = cc.ClientContactId
left join fax_contact fc on fc.ObjectID = cc.ContactPersonId
left join officeemail oe on oe.ObjectID = cc.ContactPersonId
left join personemail pe on pe.ObjectID = cc.ContactPersonId
left join Clients c on c.ClientID = cc.ClientID --company
where 1=1
and cc.ClientID not in (select ObjectId from SectorObjects where SectorId = 49) --company deleted also remove contacts
--and cc.HasLeft = 'Y' --1218 contacts left move to Default company

UNION ALL

select 'NP999999999','NP999999999','','Default','Contact','','','','','','','This is default contact from Data Import'