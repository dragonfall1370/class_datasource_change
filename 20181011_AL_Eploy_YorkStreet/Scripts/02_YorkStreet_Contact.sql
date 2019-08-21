declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

drop table if exists VCContacts

select
x.CompanyID as [contact-companyId]

, x.ContactID as [contact-externalId]

, iif(len(trim(isnull(x.Surname, ''))) = 0, 'NA', trim(isnull(x.Surname, ''))) as [contact-lastName]

, trim(isnull(x.FirstName, '')) as [contact-firstName]

, cis.Emails as [contact-email]

, [dbo].[ufn_RefinePhoneNumber](
	iif(len(trim(isnull(x.Telephone, ''))) = 0
		, iif(len(trim(isnull(x.Telephone2, ''))) = 0
			, iif(len(trim(isnull(x.DirectDialNumber, ''))) = 0
				, iif(len(trim(isnull(x.PersonalTelephone, ''))) = 0
					, iif(len(trim(isnull(x.Mobile, ''))) = 0
						, iif(len(trim(isnull(x.PersonalMobile, ''))) = 0
							, ''
							, trim(isnull(x.PersonalMobile, ''))
						)
						, trim(isnull(x.Mobile, ''))
					)
					, trim(isnull(x.PersonalTelephone, ''))
				)
				, trim(isnull(x.DirectDialNumber, ''))
			)
			, trim(isnull(x.Telephone2, ''))
		)
		, trim(isnull(x.Telephone, ''))
	)
) as [contact-phone]

, trim(isnull(x.Position, '')) as [contact-jobTitle]

, concat(
	concat('External ID: ', x.ContactId)
		, iif(x.ParentContactID is null or x.ParentContactID = 0, ''
		, concat(@DoubleNewLine, 'Parent Contact: ', cis.ParentFullName, ' - ', cis.ParentEmail)
	)
	, iif(len(trim(isnull(x.Salutation, ''))) = 0, ''
		, concat(@DoubleNewLine, 'Salutation: ', trim(isnull(x.Salutation, '')))
	)
	, concat(@DoubleNewLine, 'Decision Maker: '
		, iif(x.DecisionMaker = 1, 'Yes', 'No')
	)
	, concat(@DoubleNewLine, 'Company Address: ', @NewLineChar
		, comIdxs.FullAddress
	)
	, iif(len(trim(isnull(x.WebsiteAddress, ''))) = 0, ''
		, concat(@DoubleNewLine, 'WebsiteAddress: ', [dbo].[ufn_RefineWebAddress](x.WebsiteAddress))
	)
	, iif(len(trim(isnull(cast(x.Comments as nvarchar(max)), ''))) = 0, ''
		, concat(@DoubleNewLine, 'Comments: ', @NewLineChar
			, trim(isnull(cast(x.Comments as nvarchar(max)), ''))
		)
	)
) as [contact-Note]

, dc.Docs as [contact-document]

, cis.OwnerEmails as [company-owners]

into VCContacts

from
VCConIdxs cis
left join VCConDocs dc on cis.ConId = dc.ConId
left join Contacts x on cis.ConId = x.ContactID
left join VCComIdxs comIdxs on x.CompanyID = comIdxs.ComId

select * from VCContacts


--IF EXISTS (
--    SELECT * FROM sysobjects WHERE id = object_id(N'ufn_NumberToCharString') 
--    AND xtype IN (N'FN', N'IF', N'TF')
--)
--    DROP FUNCTION ufn_NumberToCharString

--GO

--CREATE FUNCTION [dbo].ufn_NumberToCharString (
--	@input VARCHAR(256)
--) RETURNS VARCHAR(256)
--AS  
--BEGIN
--	DECLARE @retVal VARCHAR(256)

--	SET @retVal = ''

--	declare @i int

--	set @i = 0

--	while @i < len(@input)
--	begin
--		set @i = @i + 1

--		set @retVal += char(65 + cast(substring(@input, @i, 1) as int))
--	end

--	RETURN @retVal
--END

--GO

--declare @NewLineChar as char(2) = char(13) + char(10);
--declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

--select distinct
--x.CompanyID as [contact-companyId]
--, c.ContactID as [contact-externalId]
----, coalesce(c.Surname, '') as [contact-lastName]
--, iif(len(trim(coalesce(c.Surname, ''))) = 0
--	, 'NoName ' + [dbo].ufn_NumberToCharString(cast(c.ContactID as varchar(10)))
--	, trim(coalesce(c.Surname, ''))
--) as [contact-lastName]
--, coalesce(c.FirstName, '') as [contact-firstName]
--, iif(len(trim(coalesce(c.Email, c.Email2, ''))) = 0
--	, 'NoEmail-' + cast(c.ContactID as varchar(10)) + '@noemail.com'
--	, trim(coalesce(c.Email, c.Email2, ''))
--) as [contact-email]
--, iif(len(trim(coalesce(c.Telephone, ''))) = 0
--	, iif(len(trim(coalesce(c.Telephone2, ''))) = 0
--		, iif(len(trim(coalesce(c.DirectDialNumber, ''))) = 0
--			, iif(len(trim(coalesce(c.PersonalTelephone, ''))) = 0
--				, iif(len(trim(coalesce(c.Mobile, ''))) = 0
--					, iif(len(trim(coalesce(c.PersonalMobile, ''))) = 0
--						, ''
--						, trim(coalesce(c.PersonalMobile, ''))
--					)
--					, trim(coalesce(c.Mobile, ''))
--				)
--				, trim(coalesce(c.PersonalTelephone, ''))
--			)
--			, trim(coalesce(c.DirectDialNumber, ''))
--		)
--		, trim(coalesce(c.Telephone2, ''))
--	)
--	, trim(coalesce(c.Telephone, ''))
--) as [contact-phone]
--, coalesce(c.Position, '') as [contact-jobTitle]
---- note
--, ''
--+ 'Parent Contact: ' + IIF(c.ParentContactID <> 0, trim(coalesce(pc.FirstName, '') + ' ' + trim(coalesce(pc.Surname, '')) + '     External ID: ' + cast(c.ParentContactID as varchar(20))), 'N/A')
--+ @NewLineChar + 'Salutation: ' + trim(coalesce(c.Salutation, ''))
--+ @NewLineChar + 'Decision Maker: ' + iif(c.DecisionMaker = 1, 'YES', 'NO')
--+ @NewLineChar + 'Company Address: ' + (
--	select top 1
--		trim(concat_ws(' '
--				, trim(coalesce(cd.Address1, ''))
--				, trim(coalesce(cd.Address2, ''))
--				, trim(coalesce(cd.Address3, ''))
--				, trim(coalesce(cd.Town, ''))
--				, trim(coalesce(cd.County, ''))
--				, trim(coalesce(cast(cd.CountryID as varchar(20)), ''))
--				, trim(coalesce(cd.PostCode, ''))
--			)
--		)
--	from CompanyDetails cd
--	where cd.CompanyID = c.CompanyID
--)
--+ @NewLineChar + 'Website Address: ' + trim(coalesce(c.WebsiteAddress, ''))
--+ @NewLineChar + 'Comments: ' + trim(coalesce(cast(c.Comments as nvarchar(max)), ''))
--+ @NewLineChar + 'Selected Skills: ' + coalesce((
--	select top 1
--		string_agg(
--			concat_ws(
--				@NewLineChar
--				, 'Skill: ' + coalesce(s.Description, '')
--				, 'Skill Level: ' + coalesce(sl.Description, '')
--				, 'Test Date: ' + coalesce(cast(cs.TestDate as varchar(256)), '')
--				, 'Test Score: ' + coalesce(cast(cs.TestScore as varchar(10)), '')
--				, 'Comments: ' + coalesce(cast(cs.Comments as nvarchar(max)), '')
--			)
--			, @DoubleNewLine
--		)
--	from
--	ContactSkills cs
--	join Skills s on cs.SkillID = s.SkillID
--	join SkillLevels sl on cs.SkillLevel = sl.SkillLevelID
--	where cs.ContactID = c.ContactID
--	group by cs.ContactID
--), '')

--as [contact-Note]
--, coalesce(
--	(
--		select top 1 string_agg(sfp.FileName, ',')
--		from
--		StoredFilePaths sfp
--		join RecordTypes rt on sfp.RecordTypeID = rt.RecordTypeID
--		where rt.Description like '%Contacts%' and sfp.RecordID = c.ContactID
--		group by sfp.RecordID
--	)
--	, ''
--)
--as [contact-document]
----contact-photo
----contact-linkedin
----contact-skype
----contact-owners
--from Contacts c
--left join Contacts pc On c.ParentContactID = pc.ContactID
--order by c.CompanyID, c.ContactID