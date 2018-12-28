drop table if exists [dbo].[VCContacts]

declare @NewLineChar as char(2) = char(13) + char(10);

--select * from (
select

trim(isnull(x.AccountID, '')) as [contact-companyId]

, trim(isnull(x.Id, '')) as [contact-externalId]

, iif(len(trim(isnull(x.LastName, ''))) = 0, 'No Last Name', trim(isnull(x.LastName, ''))) as [contact-lastName]
--, db-field-not-found as [contact-middleName]

, trim(isnull(x.FirstName, '')) as [contact-firstName]
--, db-field-not-found as [contact-firstNameKana]
--, db-field-not-found as [contact-lastNameKana]

, isnull(cis.Emails, '') as [contact-email]

--, trim(isnull(x.Email, '')) as [contact-email]

, replace(replace(replace(trim('.,!/ '  from
	iif(len(trim(isnull(x.phone, ''))) = 0
		, iif(len(trim(isnull(x.OtherPhone, ''))) = 0
			, iif(len(trim(isnull(x.AssistantPhone, ''))) = 0
				, iif(len(trim(isnull(x.HomePhone, ''))) = 0
					, iif(len(trim(isnull(x.MobilePhone, ''))) = 0
						, ''
						, trim(isnull(x.MobilePhone, ''))
					)
					, trim(isnull(x.HomePhone, ''))
				)
				, trim(isnull(x.AssistantPhone, ''))
			)
			, trim(isnull(x.OtherPhone, ''))
		)
		, trim(isnull(x.phone, ''))
	)
), ' ', ''), '/', ','), 'or', ',')
as [contact-phone]

, trim(coalesce(x.Title, '')) as [contact-jobTitle]

, trim(@NewLineChar from 'External ID: ' + x.Id
	+ iif(len(trim(isnull(x.ReportsToId, ''))) > 0, @NewLineChar + 'Reports To: ' +
		(select top 1 trim(isnull(FirstName, '')) + ' ' + trim(isnull(LastName, '')) + ' (External ID: ' + Id + ')'
			from [Contact] where Id = trim(x.ReportsToId))
		, '')
	+ iif(len(trim(isnull(x.AssistantPhone, ''))) > 0, @NewLineChar + 'Assistant Phone:' + @NewLineChar + trim(isnull(x.AssistantPhone, '')), '')
	+ iif(len(trim(isnull(x.AssistantName, ''))) > 0, @NewLineChar + 'Assistant Name:' + @NewLineChar + trim(isnull(x.AssistantName, '')), '')
	+ iif(len(trim(isnull(x.Assist_email__c, ''))) > 0, @NewLineChar + 'Assistant Email:' + @NewLineChar + trim(isnull(x.Assist_email__c, '')), '')
	+ iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Description:' + @NewLineChar + trim(isnull(x.Description, '')), '')
	--+ iif(len(trim(isnull(a.Type, ''))) > 0, @NewLineChar + 'Type: ' + trim(isnull(a.Type, '')), '')
	--+ iif(len(trim(isnull(a.LastModifiedById, ''))) > 0 and u.Id is not null
	--	, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
	--	, '')
)
as [contact-Note]

, isnull(cd.Docs, '') as [contact-document]

, u.Username as [contact-owners]

, cast(x.CreatedDate as datetime) as CreatedDate

--, db-field-not-found as [contact-photo]
--, trim(isnull(x.FCMS__LinkedInId__c, '')) as [contact-linkedin]
--, db-field-not-found as [contact-skype]

into [dbo].[VCContacts]

from
VCConIdxs cis -- 17476
left join [Contact] x on x.Id = cis.Id
left join VCConDocs cd on x.Id = cd.ContactId
left join [User] u on x.OwnerId = u.Id
where
--[RecordTypeId] =
--'012b0000000J2RD' --candidate -- 9049 => 8844 has attachment
--'012b0000000J2RE' -- contact -- 8427 => 5 has attachment
--and cd.Docs is not null
(len(trim(isnull(x.AccountID, ''))) > 0
and trim(isnull(x.AccountID, '')) <> '000000000000000AAA'
--and trim(isnull(x.AccountID, '')) <> '001b00000044tF3AAI'
)
order by x.CreatedDate
--) abc
--where abx.[contact-companyId] = 'vx.intergration'
--where len(abc.[contact-document]) > 0
--select 9049 + 8427 -- => 17476

--select * from RecordType
--where id = '012b0000000J2RD'
--Id	Name
--012b0000000J2RDAA0	Candidate
--012b0000000J2REAA0	Contact

--select x.Id, cd.Docs
--from [dbo].[Contact] c
--left join (
--	SELECT
--		[ParentId]
--		, string_agg([Name], ',') as Docs
--	from [dbo].[Attachment]
--	WHERE [IsDeleted] = '0'
--	GROUP BY [ParentId]
--) cd on x.Id = cd.ParentId
--where x.RecordTypeId =
--'012b0000000J2RE'
----'012b0000000J2RD'
--and cd.Docs is not null

select * from VCContacts
--where [contact-externalId] like '%DefCon'
--where [contact-companyId] not in (select [company-externalId] from VCCompanies)
order by CreatedDate