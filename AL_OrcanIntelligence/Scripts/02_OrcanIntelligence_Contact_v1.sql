declare @NewLineChar as char(2) = char(13) + char(10);

with
ContactDupCheck as (
	--select * from (
	select Id, Email, row_number() over(partition by Email order by CreatedDate) as RowNum
	from Contact
	where
	[RecordTypeId] = '012b0000000J2RE'
	--) abc where abc.RowNum > 1
)

--select * from (
select

trim(isnull(c.AccountID, '')) as [contact-companyId]

, trim(isnull(c.Id, '')) as [contact-externalId]

, iif(len(trim(isnull(c.LastName, ''))) = 0, 'N/A', trim(isnull(c.LastName, ''))) as [contact-lastName]
--, db-field-not-found as [contact-middleName]

, trim(isnull(c.FirstName, '')) as [contact-firstName]
--, db-field-not-found as [contact-firstNameKana]
--, db-field-not-found as [contact-lastNameKana]

, iif(len(trim(isnull(c.Email, ''))) = 0
	, iif(len(trim(isnull(c.AVTRRT__Other_Emails__c, ''))) = 0
		, 'NoEmail-' + c.Id + '@noemail.com'
		, trim(isnull(c.AVTRRT__Other_Emails__c, ''))
	)
	, (select top 1 iif(cdc.RowNum > 1, '(' + cast(cdc.RowNum as varchar(10)) + ')' + trim(isnull(c.Email, '')), trim(isnull(c.Email, '')))
		from ContactDupCheck cdc
		where cdc.Id = c.Id and cdc.Email = c.Email
	)
)
as [contact-email]

, replace(replace(replace(trim('.,!/ '  from
	iif(len(trim(isnull(c.phone, ''))) = 0
		, iif(len(trim(isnull(c.OtherPhone, ''))) = 0
			, iif(len(trim(isnull(c.AssistantPhone, ''))) = 0
				, iif(len(trim(isnull(c.HomePhone, ''))) = 0
					, iif(len(trim(isnull(c.MobilePhone, ''))) = 0
						, ''
						, trim(isnull(c.MobilePhone, ''))
					)
					, trim(isnull(c.HomePhone, ''))
				)
				, trim(isnull(c.AssistantPhone, ''))
			)
			, trim(isnull(c.OtherPhone, ''))
		)
		, trim(isnull(c.phone, ''))
	)
), ' ', ''), '/', ','), 'or', ',')
as [contact-phone]

, trim(coalesce(c.Title, '')) as [contact-jobTitle]

, trim(@NewLineChar from 'External ID: ' + c.Id
	+ iif(len(trim(isnull(c.ReportsToId, ''))) > 0, @NewLineChar + 'Reports To: ' +
		(select top 1 trim(isnull(FirstName, '')) + ' ' + trim(isnull(LastName, '')) + ' (external ID: ' + Id + ')'
			from [Contact] where Id = trim(c.ReportsToId))
		, '')
	--+ iif(len(trim(isnull(a.Type, ''))) > 0, @NewLineChar + 'Type: ' + trim(isnull(a.Type, '')), '')
	--+ iif(len(trim(isnull(a.LastModifiedById, ''))) > 0 and u.Id is not null
	--	, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
	--	, '')
)
as [contact-Note]
, isnull(cd.Docs, '') as [contact-document]
--, db-field-not-found as [contact-photo]
, trim(isnull(c.FCMS__LinkedInId__c, '')) as [contact-linkedin]
--, db-field-not-found as [contact-skype]
--, db-field-not-found as [contact-owners]
--, RecordTypeId

from
[Contact] c -- 17476
left join VC_Con_Docs cd on c.Id = cd.ContactId
where
[RecordTypeId] =
--'012b0000000J2RD' --candidate -- 9049 => 8844 has attachment
'012b0000000J2RE' -- contact -- 8427 => 5 has attachment
--and cd.Docs is not null
and
(len(trim(isnull(c.AccountID, ''))) > 0
and trim(isnull(c.AccountID, '')) <> '000000000000000AAA'
and trim(isnull(c.AccountID, '')) <> '001b00000044tF3AAI'
)
order by c.CreatedDate
--) abc
--where abc.[contact-companyId] = 'vc.intergration'
--where len(abc.[contact-document]) > 0
--select 9049 + 8427 -- => 17476

--select * from RecordType
--where id = '012b0000000J2RD'
--Id	Name
--012b0000000J2RDAA0	Candidate
--012b0000000J2REAA0	Contact

--select c.Id, cd.Docs
--from [dbo].[Contact] c
--left join (
--	SELECT
--		[ParentId]
--		, string_agg([Name], ',') as Docs
--	from [dbo].[Attachment]
--	WHERE [IsDeleted] = '0'
--	GROUP BY [ParentId]
--) cd on c.Id = cd.ParentId
--where c.RecordTypeId =
--'012b0000000J2RE'
----'012b0000000J2RD'
--and cd.Docs is not null