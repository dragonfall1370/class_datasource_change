-- Create a temp table that contain candidates only
Create view Temp_Candidates as
select c.* 
from contacts c left join contacts_cstm cc on c.id = cc.id_c
where current_workplace_c is null or current_workplace_c = ''
-- select * from Temp_Candidates

-- Get email address for Candidates: execute all scripts in "BNS_Candidate_GetEmailAddress" sql file

-- -----------------------------------Get candidate address
create view candidate_location_edited as 
	(select tc.id,
		ltrim(rtrim(concat(
			if(tc.primary_address_street = '' or tc.primary_address_street is NULL,'',tc.primary_address_street)
			, if(tc.primary_address_city = '' or tc.primary_address_city is NULL,'',concat(', ',tc.primary_address_city))
			, if(tc.primary_address_state = '' or tc.primary_address_state is NULL,'',concat(', ',tc.primary_address_state))
			, if(tc.primary_address_postalcode = '' or tc.primary_address_postalcode is NULL,'',concat(', ',tc.primary_address_postalcode))
			, if(tc.primary_address_country = '' or tc.primary_address_country is NULL,'',if(tc.primary_address_country = 'NL',', Netherlands',concat(', ',tc.primary_address_country))))))
		as 'locationName'
	from Temp_Candidates tc)
-- select * from candidate_location_edited

-- ----------------------------------- MAIN SCRIPT
select
	concat('BNS_',tc.id) as 'candidate-externalId'
	, if(ltrim(rtrim(tc.first_name)) = '' or ltrim(rtrim(tc.first_name)) is null, 'NoFirstName',ltrim(rtrim(tc.first_name))) as 'candidate-firstName'
	, if(ltrim(rtrim(tc.last_name)) = '' or ltrim(rtrim(tc.last_name)) is null, 'NoLastName',ltrim(rtrim(tc.last_name))) as 'candidate-lastName'
	, coalesce(cc.gewenste_functietitel_c,tc.title) as 'candidate-jobTitle1'
    , case
	 	when tce.email_address is not NULL then tce.email_address
		else concat('CandidateID-',tc.id,'@noemail.com') end as 'candidate-email'
    -- , if(tce.email_address is null, concat('CandidateID-',tc.id,'@noemail.com'),
	-- 		if(position(',' in tce.email_address) <> 0, LTRIM(RTRIM(REPLACE(left(tce.email_address,position(',' in tce.email_address)-1), ' ', ''))),
	-- 			LTRIM(RTRIM(REPLACE(tce.email_address, ' ', ''))))) as 'candidate-email'
    , if(tc.phone_mobile = '' or tc.phone_mobile is null,if(tc.phone_other = '' or tc.phone_other is null, null,tc.phone_other),tc.phone_mobile) as 'candidate-phone'
    , ltrim(rtrim(tc.phone_mobile)) as 'candidate-mobile'
    , ltrim(rtrim(tc.phone_home)) as 'candidate-homePhone'
    , ltrim(rtrim(tc.phone_work)) as 'candidate-workPhone'
    , ue.email_address as 'candidate-owners'
    , tc.birthdate as 'candidate-dob'
    , case
			when cc.geslacht_c = 'Vrouw' then 'FEMALE'
            when cc.geslacht_c = 'Man' then 'MALE'
            else '' end as 'candidate-gender'
	, cc.opleidingsniveau_c as 'candidate-education'
    , if(cc.linkedin_url_c = '' or cc.linkedin_url_c is null, if(cc.linkedin_profiel_c like '%linkedin.com%',cc.linkedin_profiel_c,null), if(cc.linkedin_url_c like '%linkedin.com%',cc.linkedin_url_c, null)) as 'candidate-linkedin'
	, if(cl.locationName = '' or cl.locationName is NULL,'',ltrim(if(left(cl.locationName,2)=', ',right(cl.locationName,length(cl.locationName)-2),cl.locationName))) as 'candidate-address'
	, if(tc.primary_address_city = '' or tc.primary_address_city is NULL,'',tc.primary_address_city) as 'candidate-city'
	, if(tc.primary_address_state = '' or tc.primary_address_state is NULL,'',tc.primary_address_state) as 'candidate-state'
	, if(tc.primary_address_postalcode = '' or tc.primary_address_postalcode is NULL,'',tc.primary_address_postalcode) as 'candidate-ZipCode'
	-- , if(tc.primary_address_street = '' or tc.primary_address_street is NULL,'','NL') as 'candidate-Country'
    , case 
		when tc.primary_address_country like '%etherland%' then 'NL'
        when tc.primary_address_country like '%ederla%' then 'NL'
        when tc.primary_address_country like '' then 'NL'
        when tc.primary_address_country like '%USA%' then 'NL'
        when tc.primary_address_country like '%Belg%' then 'BE'
        when tc.primary_address_country like '%Austra%' then 'AU'
        when tc.primary_address_country like '%Denemarken%' then 'DK'
        when tc.primary_address_country like '%uitsland%' then 'NL'
        when tc.primary_address_country like '%Finland%' then 'FI'
        when tc.primary_address_country like '%Frankrijk%' then 'FR'
        when tc.primary_address_country like '%olland%' then 'NL'
        when tc.primary_address_country like '%Ierland%' then 'IE'
        when tc.primary_address_country like '%Ireland%' then 'IE'
        when tc.primary_address_country like '%Indo%' then 'ID'
        when tc.primary_address_country like '%Germany%' then 'DE'
        when tc.primary_address_country like '%ned%' then 'NL'
        when tc.primary_address_country like '%Nedreland%' then 'NL'
        when tc.primary_address_country like '%Nedrland%' then 'NL'
        when tc.primary_address_country like '%Nerderland%' then 'NL'
        when tc.primary_address_country like '%Ridderkerk%' then 'NL'
        when tc.primary_address_country like '%NLD%' then 'NL'
        when tc.primary_address_country like '%Schotland%' then 'GB'
        when tc.primary_address_country like '%Spanje%' then 'ES'
        when tc.primary_address_country like '%UK%' then 'GB'
        when tc.primary_address_country like 'AN' then 'CW'
        when tc.primary_address_country like 'EN' then 'NL'
        when tc.primary_address_country like 'FX' then 'FR'
        when length(ltrim(rtrim(tc.primary_address_country))) = 2 then ltrim(rtrim(tc.primary_address_country))
        -- when (tc.primary_address_street is not null and tc.primary_address_street <> '') and (tc.primary_address_country is null and tc.primary_address_country = '') then 'NL'
        else '' end as 'candidate-country'
from Temp_Candidates tc left join contacts_cstm cc on tc.id = cc.id_c
                -- left join Candidate_Email_Addr cea on tc.id = cea.id
                left join user_emails_main ue on tc.assigned_user_id = ue.id
                left join candidate_location_edited cl on tc.id = cl.id
                left join Temp_Can_Emails4_main tce on tc.id = tce.id
-- where tc.primary_address_country like 'FR'
-- where tc.first_name = '' or tc.first_name is null
-- where tc.id is null -- and ac.contact_id = 'dd40aa66-be84-086b-fa6e-4d1900a3cc31'-- and ac.contact_id = '4f7765a8-8888-5312-2a73-4940f0f54d17'