declare @specialChars varchar(255) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N')%20?(*,. '
declare @NewLineChar as char(1) = char(10);

drop table if exists VC_Con

;with

ConEmails as (

	select
	ConId
	, iif(len(ConEmail) = 0 or rn = 1
		, ConEmail
		, replace(ConEmail, '@', concat('_dup_', replicate('0', 2 - len(cast(rn - 1 as varchar))), rn - 1, '@'))
	) as Email
	from (
		select
			ClientContactId as ConId
			, iif(dbo.ufn_CheckEmailAddress(lower(trim(@specialChars from isnull(Phones_PrimaryEmailAddressNum, '')))) = 1, lower(trim(@specialChars from isnull(Phones_PrimaryEmailAddressNum, ''))), '') as ConEmail
			, row_number() over (partition by iif(dbo.ufn_CheckEmailAddress(lower(trim(@specialChars from isnull(Phones_PrimaryEmailAddressNum, '')))) = 1, lower(trim(@specialChars from isnull(Phones_PrimaryEmailAddressNum, ''))), '') order by ContactPersonId, ClientId) as rn
		from [RF_Contacts_Complete]
	) x
)

--select * from ConEmails
--where ConId = 7963 + 33358
--where ConId = 33091 + 33358
--where
--ConEmail like '%_dup_%'
--ConEmail like '%a.cooper%'
--where ConEmail like '%soaring-falcon.co.uk'
--*a.brettle@ecotherm.co.uk
--SELECT TRIM( '.,!* ' FROM  '#     test    .*') AS Result;
--;with
--, ConDocs as (
--	select
--	x.CONT_ID as ConId
--	, trim(isnull(y.Name, '')) as Doc
--	from CONT_DATA_TABLE x
--	left join VC_DocsIdx y on x.CONT_ID = y.ConId
--	where len(trim(isnull(y.Name, ''))) > 0
--	--and (right(Doc, 3) = 'jpg' or right(Doc, 3) = 'png' or right(Doc, 3) = 'gif')
--	and (right(trim(isnull(y.Name, '')), 3) <> 'jpg' and right(trim(isnull(y.Name, '')), 3) <> 'png' and right(trim(isnull(y.Name, '')), 3) <> 'gif')
--)

--select * from ConDocs
--where Doc is not null
--and (right(Doc, 3) = 'jpg' or right(Doc, 3) = 'png' or right(Doc, 3) = 'gif')
--and (right(Doc, 3) <> 'jpg' and right(Doc, 3) <> 'png' and right(Doc, 3) <> 'gif')


select

trim(isnull(cast(x.clientid as varchar(20)), '')) as [contact-companyId]

, trim(isnull(cast(x.ClientContactId as varchar(20)), '')) as [contact-externalId]

, trim(isnull(nullif(isnull(x.person_contactpersonidsurname, 'No Last Name]'), ''), 'No Last Name]')) as [contact-lastName]
--, db-field-not-found as [contact-middleName]

, trim(isnull(x.person_contactpersonidpersonname, '')) as [contact-firstName]
--, db-field-not-found as [contact-firstNameKana]
--, db-field-not-found as [contact-lastNameKana]

, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(y.Email, ''), '')) as [contact-email]

, [dbo].[ufn_RefinePhoneNumber_V2](trim(isnull(x.details_phoneoffice, ''))) as [contact-phone]

, trim(isnull(x.jobtitle, '')) as [contact-jobTitle]
--, isnull(z.Doc, '') as [contact-document]

--, db-field-not-found as [contact-photo]
--, db-field-not-found as [contact-linkedin]
--, db-field-not-found as [contact-skype]

, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(x.users_createdemailaddress, ''), '')) as [contact-owners]

, concat(
	concat('External ID: ', x.ContactPersonId)
	, concat('External Client ID: ', x.ClientId)
	, nullif(concat(@NewLineChar, 'Contact type: ', trim(@specialChars from isnull(x.contacttypes_description, ''))), concat(@NewLineChar, 'Contact type: '))
	, nullif(concat(@NewLineChar, 'Created on: ', FORMAT(x.createdon, 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Created on: '))
	, nullif(concat(@NewLineChar, 'Has left: ', x.hasleft), concat(@NewLineChar, 'Has left: '))
	, nullif(concat(@NewLineChar, 'Leaving date: ', FORMAT(x.leavingdate, 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Leaving date: '))
	, nullif(concat(@NewLineChar, 'Salutation: ', trim(@specialChars from isnull(x.person_contactpersonidsalutation, ''))), concat(@NewLineChar, 'Salutation: '))
	, nullif(concat(@NewLineChar, 'Title: ', trim(@specialChars from isnull(z.listvalues_title, ''))), concat(@NewLineChar, 'Title: '))
	, nullif(concat(@NewLineChar, 'Alternative email: ', trim(@specialChars from isnull(x.details_email, ''))), concat(@NewLineChar, 'Alternative email: '))
	, nullif(concat(@NewLineChar, 'Personal email: ', trim(@specialChars from isnull(x.details_emailpersonal, ''))), concat(@NewLineChar, 'Personal email: '))
	, nullif(concat(@NewLineChar, 'Alternative phone: ', trim(@specialChars from isnull(x.details_phone, ''))), concat(@NewLineChar, 'Alternative phone: '))
	, nullif(concat(@NewLineChar, 'Phone day: ', trim(@specialChars from isnull(x.details_phoneday, ''))), concat(@NewLineChar, 'Phone day: '))
	, nullif(concat(@NewLineChar, 'Position name: ', trim(@specialChars from isnull(x.listvalues_positionvalueidvaluename, ''))), concat(@NewLineChar, 'Position name: '))
	, nullif(concat(@NewLineChar, 'Position: ', trim(@specialChars from isnull(x.listvalues_positionvalueiddescription, ''))), concat(@NewLineChar, 'Position: '))
	, nullif(concat(@NewLineChar, 'Employment start date: ', FORMAT(x.employmentstartdate, 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Employment start date: '))
	, nullif(concat(@NewLineChar, 'Employment end date: ', FORMAT(x.employmentenddate, 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Employment end date: '))
	, nullif(concat(@NewLineChar, 'Gender: ', trim(@specialChars from isnull(a.listvalues_gender, ''))), concat(@NewLineChar, 'Gender: '))
	, nullif(concat(@NewLineChar, 'Nationality: ', x.person_contactpersonidnationalityid), concat(@NewLineChar, 'Nationality: '))
	, nullif(concat(@NewLineChar, 'DOB: ', FORMAT(x.person_contactpersoniddob, 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'DOB: '))
	, nullif(concat(@NewLineChar, 'Notes: ', trim(isnull(x.person_contactpersonidnotes, ''))), concat(@NewLineChar, 'Notes: '))

) as [contact-Note]

into VC_Con

from
[RF_Contacts_Complete] x
left join ConEmails y on x.ClientContactId = y.ConId
LEFT JOIN (SELECT ListValueId, ValueName as listvalues_title FROM ListValues) z ON x.details_titlevalueid = z.ListValueId
LEFT JOIN (SELECT ListValueId, ValueName as listvalues_gender FROM ListValues) a ON x.details_gendervalueid = a.ListValueId;
--left join ConDocs z on x.CONT_ID = z.ConId

select * from VC_Con
--where len([contact-document]) > 0
--where [contact-externalId] = '73771'
--where [contact-phone] like '%+44%'
--where [contact-companyId] = '_DefCom000'
--where [contact-email] = 'paulw@courthouseclinics.com'
--where [contact-firstName] like '\[Default Contact%' escape '\'
--where [contact-companyId] <> '_DefCom000' and [contact-firstName] not like '\[Default Contact%' escape '\'
--where [contact-companyId] not in (select [company-externalId] from VC_Com)
order by cast([contact-externalId] as int)
--order by [contact-lastName]

--select * from VC_Con
--where [contact-companyId] not in (
--	select [company-externalId]
--	from VC_Com
--)

--update VC_Con
--set [contact-companyId] = 'DefCom000'
--where [contact-externalId] in (
--	select [contact-externalId]
--	from VC_Con
--	where [contact-companyId] not in (
--		select [company-externalId]
--		from VC_Com
--	)
--)

--select trim(char(9) from 'david.scrivener@ensors.co.uk	')

--insert into VC_Con values (
--'_DefCom000'
--, '_DefCon000'
--, 'Contact 00]'
--, '[Default'
--, ''
--, ''
--, ''
--, ''
--, ''
--, 'ExternalID: _DefCon000'
--)

--update VC_Con
--set [contact-Note] = 'ExternalID: _DefCon000'
--where [contact-externalId] = '_DefCon000'

--select * from VC_Con
--where [contact-externalId] like '_DefCon%'