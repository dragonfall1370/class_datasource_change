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
			CONT_ID as ConId
			, iif(dbo.ufn_CheckEmailAddress(lower(trim(@specialChars from isnull(EMAIL, '')))) = 1, lower(trim(@specialChars from isnull(EMAIL, ''))), '') as ConEmail
			, row_number() over (partition by iif(dbo.ufn_CheckEmailAddress(lower(trim(@specialChars from isnull(EMAIL, '')))) = 1, lower(trim(@specialChars from isnull(EMAIL, ''))), '') order by CONT_ID) as rn
		from CONT_DATA_TABLE
	) x
)

--select * from ConEmails
--where
--ConEmail like '%_dup_%'
--ConEmail like '%a.cooper%'
--where ConEmail like '%soaring-falcon.co.uk'
--*a.brettle@ecotherm.co.uk
--SELECT TRIM( '.,!* ' FROM  '#     test    .*') AS Result;
--;with
, ConDocs as (
	select
	x.CONT_ID as ConId
	, trim(isnull(y.Name, '')) as Doc
	from CONT_DATA_TABLE x
	left join VC_DocsIdx y on x.CONT_ID = y.ConId
	where len(trim(isnull(y.Name, ''))) > 0
	--and (right(Doc, 3) = 'jpg' or right(Doc, 3) = 'png' or right(Doc, 3) = 'gif')
	and (right(trim(isnull(y.Name, '')), 3) <> 'jpg' and right(trim(isnull(y.Name, '')), 3) <> 'png' and right(trim(isnull(y.Name, '')), 3) <> 'gif')
)

--select * from ConDocs
--where Doc is not null
--and (right(Doc, 3) = 'jpg' or right(Doc, 3) = 'png' or right(Doc, 3) = 'gif')
--and (right(Doc, 3) <> 'jpg' and right(Doc, 3) <> 'png' and right(Doc, 3) <> 'gif')


select

trim(isnull(cast(x.CLNT_ID as varchar(20)), '')) as [contact-companyId]

, trim(isnull(cast(x.CONT_ID as varchar(20)), '')) as [contact-externalId]

, trim(isnull(nullif(isnull(x.SURNAME, 'No Last Name]'), ''), 'No Last Name]')) as [contact-lastName]
--, db-field-not-found as [contact-middleName]

, trim(isnull(x.FORENAME, '')) as [contact-firstName]
--, db-field-not-found as [contact-firstNameKana]
--, db-field-not-found as [contact-lastNameKana]

, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(y.Email, ''), '')) as [contact-email]

, [dbo].[ufn_RefinePhoneNumber_V2](trim(isnull(x.PHONE, ''))) as [contact-phone]

, trim(isnull(x.POSITION, '')) as [contact-jobTitle]
, isnull(z.Doc, '') as [contact-document]

--, db-field-not-found as [contact-photo]
--, db-field-not-found as [contact-linkedin]
--, db-field-not-found as [contact-skype]

, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(x.USER_ID, ''), '')) as [contact-owners]

, concat(
	--concat('External ID: ', x.CAND_ID)
	nullif(concat(@NewLineChar, 'Entered: ', FORMAT(dateadd(day, -2, cast(x.Entered as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Entered: '))
	, nullif(concat(@NewLineChar, 'Salutation: ', trim(@specialChars from isnull(x.salutation, ''))), concat(@NewLineChar, 'Salutation: '))
) as [contact-Note]

into VC_Con

from
CONT_DATA_TABLE x
left join ConEmails y on x.CONT_ID = y.ConId
left join ConDocs z on x.CONT_ID = z.ConId

select * from VC_Con
--where len([contact-document]) > 0
--where [contact-externalId] = '73771'
--where [contact-phone] like '%+44%'
--where [contact-companyId] = '_DefCom000'
--where [contact-email] = 'paulw@courthouseclinics.com'
--where [contact-firstName] like '\[Default Contact%' escape '\'
where [contact-companyId] <> '_DefCom000' and [contact-firstName] not like '\[Default Contact%' escape '\'
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