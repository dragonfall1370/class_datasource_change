-----------------------------------------------------------
drop function if exists [dbo].[ufn_RefinePhoneNumber]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_RefinePhoneNumber] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)

	set @retVal =
	replace(
		replace(
			--replace(
				replace(
					trim('.,!/ ' from isnull(@input, ''))
					, ' '
					, ''
				)
			--	, '-'
			--	, ''
			--)
			, '//'
			, ','
		)
		, '/'
		, ','
	)

	return @retVal
end

go

select
Id as ConExtId
--, AssistantPhone
--, AVTRRT__Cell_Phone__c
--, AVTRRT__Phone__c
, HomePhone
, concat(
	[dbo].[ufn_RefinePhoneNumber](MobilePhone)
	, iif(len([dbo].[ufn_RefinePhoneNumber](OtherPhone)) > 0
		, ',' + [dbo].[ufn_RefinePhoneNumber](OtherPhone)
		, '')
) as mobile_phone
--, MobilePhone
, [dbo].[ufn_RefinePhoneNumber](HomePhone) as home_phone
, OtherPhone
--, [dbo].[ufn_RefinePhoneNumber](OtherPhone) as other_phone
--, Phone

from Contact

where

IsDeleted = 0
and RecordTypeId = '012b0000000J2RE'
and (
	len(trim(isnull(HomePhone, ''))) > 0
	or len(trim(isnull(MobilePhone, ''))) > 0
	or len(trim(isnull(OtherPhone, ''))) > 0
)
-- select * from RecordType
-- Candidate: 012b0000000J2RD
-- Contact: 012b0000000J2RE

--select concat(null, null)

--select '' + null