declare @NewLineChar as char(2) = char(13) + char(10);

declare @DoubleLine as char(4) = @NewLineChar + @NewLineChar;

--select top 10 c.*
--from company c join note n on c.contactId = n.contactId

--select
--cast(nss.CR_extId as varchar(20)) as CR_extId,
--string_agg(nss.CR_note, @DoubleLine) as CR_note
--from (
--	select
--	ns.CR_extId
--	, ns.CR_note
--	, row_number() over(partition by CR_extId order by CR_extId) as rowNum
--	from (
--		select
--		c.contactId as CR_extId
--		, concat_ws(
--			@NewLineChar
--			, 'Tags: ' + NULLIF(c.tags, '')
--			, 'Home Location Address: ' + isnull(ah.street, '')
--			, 'Home Location City: ' + isnull(ah.city, '')
--			, 'Home Location State: ' + isnull(ah.state, '')
--			, 'Home Location ZIP / Postal: ' + isnull(ah.zipCode, '')
--			, 'Home Location Country: ' + isnull(ah.country, '')
--			, 'Mobile Phone: ' + isnull(pm.phoneNumber, '')
--			, 'Fax: ' + isnull(pf.phoneNumber, '')
--			, 'Home Phone: ' + NULLIF(ph.phoneNumber, '')
--		) as CR_note
--		from
--		company c
--		left join [address.home] ah on c.contactId = ah.contactId
--		left join [phone.mobile] pm on c.contactId = pm.contactId
--		left join [phone.fax] pf on c.contactId = pf.contactId
--		left join [phone.home] ph on c.contactId = ph.contactId
--	) ns
--) nss
--group by CR_extId

---- contact
--with
--ContactIndexs AS (
--	SELECT
--	contactId
--	FROM person
--	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, '|') WHERE UPPER(VALUE) = UPPER('client'))  = 1
--)

--select
--cast(nss.CR_extId as varchar(20)) as CR_extId,
--string_agg(nss.CR_note, @DoubleLine) as CR_note
--from (
--	select
--	ns.CR_extId
--	, ns.CR_note
--	, row_number() over(partition by CR_extId order by CR_extId) as rowNum
--	from (
--		select
--		c.contactId as CR_extId
--		, concat_ws(
--			@NewLineChar
--			, 'Source: ' + isnull(p.source, '')
--			, 'Tags: ' + isnull(p.tags, '')
--			, 'Home Location Address: ' + NULLIF(ah.street, '')
--			, 'Home Location City: ' + NULLIF(ah.city, '')
--			, 'Home Location State: ' + NULLIF(ah.state, '')
--			, 'Home Location Country: ' + NULLIF(ah.country, '')
--			, 'Home Location ZIP / Postal: ' + isnull(ah.zipCode, '')
--			, 'Work Phone: ' + isnull(pw.phoneNumber, '') + isnull(pw.extension, '')
--			, 'Home Phone: ' + isnull(ph.phoneNumber, '') + isnull(ph.extension, '')
--			, 'Fax: ' + isnull(pf.phoneNumber, '')
--			, 'Website: ' + isnull(wh.url, '')
--		) as CR_note
--		from
--		ContactIndexs c
--		left join person p on c.contactId = p.contactId
--		left join [address.home] ah on c.contactId = ah.contactId
--		left join [phone.mobile] pm on c.contactId = pm.contactId
--		left join [phone.fax] pf on c.contactId = pf.contactId
--		left join [phone.home] ph on c.contactId = ph.contactId
--		left join [phone.work] pw on c.contactId = pw.contactId
--		left join [website.home] wh on c.contactId = wh.contactId
--	) ns
--) nss
--group by CR_extId

-- candidate
with
CandidateIndexs AS (
	SELECT
	contactId
	FROM person
	WHERE (SELECT COUNT(VALUE) FROM STRING_SPLIT(tags, '|') WHERE UPPER(VALUE) = UPPER('client'))  = 0
)

select
cast(c.contactId as varchar(20)) as CR_extId
, concat_ws(
	@NewLineChar
	, 'Source: ' + isnull(p.source, '')
	, 'Tags: ' + isnull(p.tags, '')
	, 'Home Location Address: ' + isnull(ah.street, '')
	, 'Home Location City: ' + isnull(ah.city, '')
	, 'Home Location State: ' + isnull(ah.state, '')
	, 'Home Location Country: ' + isnull(ah.country, '')
	, 'Home Location ZIP / Postal: ' + isnull(ah.zipCode, '')
	, 'Work Phone: ' + isnull(pw.phoneNumber, '') + isnull(pw.extension, '')
	, 'Home Phone: ' + isnull(ph.phoneNumber, '') + isnull(ph.extension, '')
	, 'Fax: ' + isnull(pf.phoneNumber, '')
	, 'Website: ' + isnull(wh.url, '')
	, 'Level of Expertise: ' + isnull(pn1._Level_of_Expertise_, '')
	, 'Often clients ask us for specific project exposure: ' + COALESCE(pn1._Often_clients_ask_us_for_specific_project_exposure, '')
	, 'In which country are you currently located: ' + COALESCE(pn1._In_which_country_are_you_currently_located__,
		pn2._In_which_country_are_you_currently_located__, pn3._In_which_country_are_you_currently_located__, '')
	, 'How quickly could you mobilize to New Zealand: ' + COALESCE(pn1._How_quickly_could_you_mobilize_to_New_Zealand__,
		pn2._How_quickly_could_you_mobilise_to_New_Zealand__, pn3._How_quickly_could_you_mobilise_to_New_Zealand__, '')
	, 'Where in New Zealand would you consider roles: ' + COALESCE(pn1._Where_in_New_Zealand_would_you_consider_roles__,
		pn2._Where_in_New_Zealand_would_you_consider_roles__, pn3._Where_in_New_Zealand_would_you_consider_roles__, '')
	, 'Specialisation: ' + isnull(pn2.Specialisation, '')
	, 'In which type of environments have you worked: ' + isnull(pn2._In_which_type_of_environments_have_you_worked__, '')
	, 'How many years of experience in Engineering Consultants: ' + isnull(pn2._How_many_years_of_experience_in_Engineering_Consultants__, '')
	, 'How many years of experience in Local Authority: ' + isnull(pn2._How_many_years_of_experience_in_Local_Authority__, '')
	, 'How many years of experience in Government Agency: ' + isnull(pn2._How_many_years_of_experience_in_Government_Agency__, '')
	, 'Level of Expertise: ' + isnull(pn2._Level_of_Expertise_, '')
	, 'Head Office Based: ' + isnull(pn3._Head_Office_Based_, '')
	, 'Operational or Site based: ' + isnull(pn3._Operational_or_Site_based_, '')
	, 'Often clients ask us for specific project exposure: ' + isnull(pn3._Often_clients_ask_us_for_specific_project_exposure, '')
	, 'Reason for candidate pulling out of Catalyst referral process: ' + isnull(pn4._Reason_for_candidate_pulling_out_of_Catalyst_referral_process_, '')

) as CR_note
from
CandidateIndexs c
left join person p on c.contactId = p.contactId
left join [address.home] ah on c.contactId = ah.contactId
left join [phone.mobile] pm on c.contactId = pm.contactId
left join [phone.fax] pf on c.contactId = pf.contactId
left join [phone.home] ph on c.contactId = ph.contactId
left join [phone.work] pw on c.contactId = pw.contactId
left join [website.home] wh on c.contactId = wh.contactId
left join [dbo].[Persons.CVUpdatePortalStage2ProfessionalProjectManagementID233] pn1 ON c.contactId = pn1._Person_ID_
left join [dbo].[Persons.CVUpdatePortalStage2EngineeringLocalAuthorityGovernmentID215] pn2 ON c.contactId = pn2._Person_ID_
left join [dbo].[Persons.CVUpdatePortalStage2ConstructionID213] pn3 ON c.contactId = pn3._Person_ID_
left join [dbo].[Persons.CandidateNotesReasonsforpullingoutofreferralprocessID210] pn4 ON c.contactId = pn4._Person_ID_

select
cast(nss.CR_extId as varchar(20)) as CR_extId,
string_agg(nss.CR_note, @DoubleLine) as CR_note
from (
	select
	ns.CR_extId
	, ns.CR_note
	, row_number() over(partition by CR_extId order by CR_extId) as rowNum
	from (
		select
		c.contactId as CR_extId
		, concat_ws(
			@NewLineChar
			, 'Source: ' + isnull(p.source, '')
			, 'Tags: ' + isnull(p.tags, '')
			, 'Home Location Address: ' + isnull(ah.street, '')
			, 'Home Location City: ' + isnull(ah.city, '')
			, 'Home Location State: ' + isnull(ah.state, '')
			, 'Home Location Country: ' + isnull(ah.country, '')
			, 'Home Location ZIP / Postal: ' + isnull(ah.zipCode, '')
			, 'Work Phone: ' + isnull(pw.phoneNumber, '') + isnull(pw.extension, '')
			, 'Home Phone: ' + isnull(ph.phoneNumber, '') + isnull(ph.extension, '')
			, 'Fax: ' + isnull(pf.phoneNumber, '')
			, 'Website: ' + isnull(wh.url, '')
			, 'Level of Expertise: ' + isnull(pn1._Level_of_Expertise_, '')
			, 'Often clients ask us for specific project exposure: ' + COALESCE(pn1._Often_clients_ask_us_for_specific_project_exposure, '')
			, 'In which country are you currently located: ' + COALESCE(pn1._In_which_country_are_you_currently_located__,
				pn2._In_which_country_are_you_currently_located__, pn3._In_which_country_are_you_currently_located__, '')
			, 'How quickly could you mobilize to New Zealand: ' + COALESCE(pn1._How_quickly_could_you_mobilize_to_New_Zealand__,
				pn2._How_quickly_could_you_mobilise_to_New_Zealand__, pn3._How_quickly_could_you_mobilise_to_New_Zealand__, '')
			, 'Where in New Zealand would you consider roles: ' + COALESCE(pn1._Where_in_New_Zealand_would_you_consider_roles__,
				pn2._Where_in_New_Zealand_would_you_consider_roles__, pn3._Where_in_New_Zealand_would_you_consider_roles__, '')
			, 'Specialisation: ' + isnull(pn2.Specialisation, '')
			, 'In which type of environments have you worked: ' + isnull(pn2._In_which_type_of_environments_have_you_worked__, '')
			, 'How many years of experience in Engineering Consultants: ' + isnull(pn2._How_many_years_of_experience_in_Engineering_Consultants__, '')
			, 'How many years of experience in Local Authority: ' + isnull(pn2._How_many_years_of_experience_in_Local_Authority__, '')
			, 'How many years of experience in Government Agency: ' + isnull(pn2._How_many_years_of_experience_in_Government_Agency__, '')
			, 'Level of Expertise: ' + isnull(pn2._Level_of_Expertise_, '')
			, 'Head Office Based: ' + isnull(pn3._Head_Office_Based_, '')
			, 'Operational or Site based: ' + isnull(pn3._Operational_or_Site_based_, '')
			, 'Often clients ask us for specific project exposure: ' + isnull(pn3._Often_clients_ask_us_for_specific_project_exposure, '')
			, 'Reason for candidate pulling out of Catalyst referral process: ' + isnull(pn4._Reason_for_candidate_pulling_out_of_Catalyst_referral_process_, '')

		) as CR_note
		from
		CandidateIndexs c
		left join person p on c.contactId = p.contactId
		left join [address.home] ah on c.contactId = ah.contactId
		left join [phone.mobile] pm on c.contactId = pm.contactId
		left join [phone.fax] pf on c.contactId = pf.contactId
		left join [phone.home] ph on c.contactId = ph.contactId
		left join [phone.work] pw on c.contactId = pw.contactId
		left join [website.home] wh on c.contactId = wh.contactId
		left join [dbo].[Persons.CVUpdatePortalStage2ProfessionalProjectManagementID233] pn1 ON c.contactId = pn1._Person_ID_
		left join [dbo].[Persons.CVUpdatePortalStage2EngineeringLocalAuthorityGovernmentID215] pn2 ON c.contactId = pn2._Person_ID_
		left join [dbo].[Persons.CVUpdatePortalStage2ConstructionID213] pn3 ON c.contactId = pn3._Person_ID_
		left join [dbo].[Persons.CandidateNotesReasonsforpullingoutofreferralprocessID210] pn4 ON c.contactId = pn4._Person_ID_
	) ns
) nss
group by CR_extId