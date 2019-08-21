drop table if exists [dbo].[VCCandidates]

declare @NewLineChar as char(1) = char(10);
declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;

--select * from (
select

trim(isnull(x.Id, '')) as [candidate-externalId]

, upper(replace(trim(isnull(x.Salutation, '')), '.', '')) as [candidate-title]

, iif(len(trim(isnull(x.FirstName, ''))) = 0, 'No First Name', trim(isnull(x.FirstName, ''))) as [candidate-firstName]
--, db-field-not-found as [candidate-middleName]

, iif(len(trim(isnull(x.LastName, ''))) = 0, 'No Last Name', trim(isnull(x.LastName, ''))) as [candidate-lastname]
--, db-field-not-found as [candidate-FirstNameKana]
--, db-field-not-found as [candidate-LastNameKana]

--, iif(
--	len(trim(cis.Emails)) = 0
--	, concat('NoEmail-', x.Id, '@noemail.com')
--	, isnull(cis.Emails, '')
--)
, trim(isnull(cis.Emails, '')) as [candidate-email]

--, db-field-not-found as [candidate-workEmail]

--, db-field-not-found as [candidate-employmentType]
--, db-field-not-found as [candidate-jobTypes]

--, trim(isnull(x.AVTRRT__Gender__c, '')) as [candidate-gender]

, trim(isnull(convert(varchar(50), cast(x.Birthdate as datetime), 111), '')) as [candidate-dob]

-- populate full address
, trim('., ' from concat(
		trim(isnull(x.MailingStreet, ''))
		, iif(len(trim('., ' from isnull(x.MailingCity, ''))) > 0, ', ' + trim('., ' from isnull(x.MailingCity, '')), '')
		, iif(len(trim('., ' from isnull(x.MailingState, ''))) > 0, ', ' + trim('., ' from isnull(x.MailingState, '')), '')
		, iif(len(trim('., ' from isnull(x.MailingPostalCode, ''))) > 0, ', ' + trim('., ' from isnull(x.MailingPostalCode, '')), '')
		, iif(len(trim('., ' from isnull(x.MailingCountry, ''))) > 0, ', ' + trim('., ' from isnull(x.MailingCountry, '')), '')
		)
)  as [candidate-address]

--, trim(isnull(x.MailingStreet, '')) as [candidate-address]

, trim(isnull(x.MailingCity, '')) as [candidate-city]

, trim(isnull(x.MailingState, '')) as [candidate-State]

, iif(len(trim(isnull(x.MailingCountry, ''))) > 0
	, iif(upper(trim(isnull(x.MailingCountry, ''))) = 'UK'
		, 'GB'
		, isnull((select top 1 [Code] from [VCCountries] ccd 
				where lower(ccd.Name) like lower(trim(isnull(x.MailingCountry, '')))
				or lower(ccd.Code) like lower(trim(isnull(x.MailingCountry, '')))), 'GB')
	)
	, ''
) as [candidate-Country]

, trim(isnull(x.MailingPostalCode, '')) as [candidate-zipCode]

, replace(replace(replace(trim('.,!/ '  from
	iif(len(trim(isnull(x.Phone, ''))) = 0
		, iif(len(trim(isnull(x.MobilePhone, ''))) = 0
			, iif(len(trim(isnull(x.AssistantPhone, ''))) = 0
				, iif(len(trim(isnull(x.HomePhone, ''))) = 0
					, iif(len(trim(isnull(x.OtherPhone, ''))) = 0
						, ''
						, trim(isnull(x.OtherPhone, ''))
					)
					, trim(isnull(x.HomePhone, ''))
				)
				, trim(isnull(x.AssistantPhone, ''))
			)
			, trim(isnull(x.MobilePhone, ''))
		)
		, trim(isnull(x.Phone, ''))
	)
), ' ', ''), '/', ','), 'or', ',')
as [candidate-phone]

, replace(replace(replace(trim('.,!/ '  from
	trim(isnull(x.HomePhone, ''))
), ' ', ''), '/', ','), 'or', ',') as [candidate-homePhone]

, replace(replace(replace(trim('.,!/ '  from
	trim(isnull(x.OtherPhone, ''))
), ' ', ''), '/', ','), 'or', ',') as [candidate-workPhone]

, replace(replace(replace(trim('.,!/ '  from
	trim(isnull(x.MobilePhone, ''))
), ' ', ''), '/', ','), 'or', ',') as [candidate-mobile]

--, trim(isnull(x.OtherPhone, '')) as [candidate-workPhone]

--, trim(isnull(x.MobilePhone, '')) as [candidate-mobile]

--, [Nationality__c] as [candidate-citizenship]
-- data too chaos

--, trim(isnull(x.FCMS__LinkedInId__c, '')) as [candidate-linkedln]

--, 'GBP' as [candidate-currentSalary]
--, db-field-not-found as [candidate-desiredSalary]
--, db-field-not-found as [candidate-contractInterval]
--, db-field-not-found as [candidate-contractRate]
, 'GBP' as [candidate-currency]

--, db-field-not-found as [candidate-degreeName]
--, db-field-not-found as [candidate-education]
--, db-field-not-found as [candidate-educationLevel]
--, db-field-not-found as [candidate-gpa]
--, db-field-not-found as [candidate-grade]
--, db-field-not-found as [candidate-graduationDate]
--, db-field-not-found as [candidate-schoolName]

, trim(', ' from
	concat(
		iif(len(trim(isnull(Main_Competitors__c, ''))) = 0, ''
			, 'Main Competitors: ' + @NewLineChar + trim(isnull(Main_Competitors__c, ''))
		)
		, iif(len(trim(isnull(What_are_they_selling__c, ''))) = 0, ''
			, @DoubleNewLine + 'What are they selling: ' + @NewLineChar + trim(isnull(What_are_they_selling__c, ''))
		)
		, iif(len(trim(isnull(Sales_Targets__c, ''))) = 0, ''
			, @DoubleNewLine + 'Sales Targets:' + @NewLineChar + trim(isnull(Sales_Targets__c, ''))
		)
		, iif(len(trim(isnull(Example_of_3_deals__c, ''))) = 0, ''
			, @DoubleNewLine + 'Example of 3 deals:' + @NewLineChar + trim(isnull(Example_of_3_deals__c, ''))
		)
		, iif(len(trim(isnull(Prizes_Achievements__c, ''))) = 0, ''
			, @DoubleNewLine + 'Prizes, Achievements:' + @NewLineChar + trim(isnull(Prizes_Achievements__c, ''))
		)
		, iif(len(trim(isnull(What_are_they_selling__c, ''))) = 0, ''
			, @DoubleNewLine + 'Basic Salary + OTE + Bens:' + @NewLineChar + trim(isnull(What_are_they_selling__c, ''))
		)
		, iif(len(trim(isnull(Reports_to_1__c, ''))) = 0, ''
			, @DoubleNewLine + 'Reports to:' + @NewLineChar + trim(isnull(Reports_to_1__c, ''))
		)
		, iif(len(trim(isnull(Languages__c, ''))) = 0, ''
			, @DoubleNewLine + 'Languages:' + @NewLineChar + trim(isnull(Languages__c, ''))
		)
		, iif(len(trim(isnull(Who_are_they_selling_to__c, ''))) = 0, ''
			, @DoubleNewLine + 'Who are they selling to:' + @NewLineChar + trim(isnull(Who_are_they_selling_to__c, ''))
		)
		, iif(len(trim(isnull(Order_values_Sales_cycles_LoN__c, ''))) = 0, ''
			, @DoubleNewLine + 'Order values/Sales cycles:' + @NewLineChar + trim(isnull(Order_values_Sales_cycles_LoN__c, ''))
		)
		, iif(len(trim(isnull(NBvAM_Negotiation_Level__c, ''))) = 0, ''
			, @DoubleNewLine + 'NBvAM/Negotiation Level:' + @NewLineChar + trim(isnull(NBvAM_Negotiation_Level__c, ''))
		)
		, iif(len(trim(isnull(What_else_can_you_tell_me__c	, ''))) = 0, ''
			, @DoubleNewLine + 'What else can you tell me:' + @NewLineChar + trim(isnull(What_else_can_you_tell_me__c	, ''))
		)
		, iif(len(trim(isnull(End_of_Financial_Year_CONTACT__c, ''))) = 0, ''
			, @DoubleNewLine + 'End of Financial Year:' + @NewLineChar + trim(isnull(End_of_Financial_Year_CONTACT__c, ''))
		)
		, iif(len(trim(isnull(RFL__c, ''))) = 0, ''
			, @DoubleNewLine + 'RFL: ' + @NewLineChar + trim(isnull(RFL__c, ''))
		)
		, iif(len(trim(isnull(What_earnt_in_last_3_years__c, ''))) = 0, ''
			, @DoubleNewLine + 'What earnt in last 3 years:' + @NewLineChar + trim(isnull(What_earnt_in_last_3_years__c, ''))
		)
	)
) as [candidate-company1]
, trim(isnull([Company_Name__c], '')) as [candidate-employer1]
, trim(isnull([Job_Tiltle__c], '')) as [candidate-jobTitle1]
, trim(isnull(convert(varchar(50), cast(x.[Date_Joined__c] as datetime), 111), '')) as [candidate-startDate1]
, trim(isnull(convert(varchar(50), cast(x.[Date_Employment_ended__c] as datetime), 111), '')) as [candidate-endDate1]

, trim(', ' from
	concat(
		iif(len(trim(isnull(What_are_they_selling_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'What are they selling: ' + @NewLineChar + trim(isnull(What_are_they_selling_2__c, ''))
		)
		, iif(len(trim(isnull(Sales_targets_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Sales Targets:' + @NewLineChar + trim(isnull(Sales_targets_2__c, ''))
		)
		, iif(len(trim(isnull(Example_of_3_deals_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Example of 3 deals:' + @NewLineChar + trim(isnull(Example_of_3_deals_2__c, ''))
		)
		, iif(len(trim(isnull(Basic_Salary_ote_bens_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Prizes, Achievements:' + @NewLineChar + trim(isnull(Basic_Salary_ote_bens_2__c, ''))
		)
		, iif(len(trim(isnull(Prizes_Achievements_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Basic Salary + OTE + Bens:' + @NewLineChar + trim(isnull(Prizes_Achievements_2__c, ''))
		)
		, iif(len(trim(isnull(Reports_to_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Reports to:' + @NewLineChar + trim(isnull(Reports_to_2__c, ''))
		)
		, iif(len(trim(isnull(Who_are_they_selling_to_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Who are they selling to:' + @NewLineChar + trim(isnull(Who_are_they_selling_to_2__c, ''))
		)
		, iif(len(trim(isnull(Order_values_sales_cycles_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'Order values/Sales cycles:' + @NewLineChar + trim(isnull(Order_values_sales_cycles_2__c, ''))
		)
		, iif(len(trim(isnull(NB_v_AM_Negotiation_level_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'NBvAM/Negotiation Level:' + @NewLineChar + trim(isnull(NB_v_AM_Negotiation_level_2__c, ''))
		)
		, iif(len(trim(isnull(What_else_can_you_tell_me_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'What else can you tell me:' + @NewLineChar + trim(isnull(What_else_can_you_tell_me_2__c, ''))
		)
		, iif(len(trim(isnull(RFL_2__c, ''))) = 0, ''
			, @DoubleNewLine + 'RFL: ' + @NewLineChar + trim(isnull(RFL_2__c, ''))
		)
	)
) as [candidate-company2]
, trim(isnull([Compamy_Name__c], '')) as [candidate-employer2]
, trim(isnull([Job_title_2__c], '')) as [candidate-jobTitle2]
, trim(isnull(convert(varchar(50), cast(x.[Date_Joined_2__c] as datetime), 111), '')) as [candidate-startDate2]
, trim(isnull(convert(varchar(50), cast(x.[Date_employment_ended_2__c] as datetime), 111), '')) as [candidate-endDate2]

, trim(', ' from
	concat(
		iif(len(trim(isnull(What_are_they_selling_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'What are they selling: ' + @NewLineChar + trim(isnull(What_are_they_selling_3__c, ''))
		)
		, iif(len(trim(isnull(Sales_targets_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Sales Targets:' + @NewLineChar + trim(isnull(Sales_targets_3__c, ''))
		)
		, iif(len(trim(isnull(Example_of_deals_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Example of 3 deals:' + @NewLineChar + trim(isnull(Example_of_deals_3__c, ''))
		)
		, iif(len(trim(isnull(Prizes_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Prizes, Achievements:' + @NewLineChar + trim(isnull(Prizes_3__c, ''))
		)
		, iif(len(trim(isnull(Basic_salary_ote_bens_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Basic Salary + OTE + Bens:' + @NewLineChar + trim(isnull(Basic_salary_ote_bens_3__c, ''))
		)
		, iif(len(trim(isnull(Who_report_to_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Reports to:' + @NewLineChar + trim(isnull(Who_report_to_3__c, ''))
		)
		, iif(len(trim(isnull(What_are_they_selling_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Who are they selling to:' + @NewLineChar + trim(isnull(What_are_they_selling_3__c, ''))
		)
		, iif(len(trim(isnull(Sales_cycles_Order_values_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'Order values/Sales cycles:' + @NewLineChar + trim(isnull(Sales_cycles_Order_values_3__c, ''))
		)
		, iif(len(trim(isnull(NB_v_AM_Negotiation_level__c, ''))) = 0, ''
			, @DoubleNewLine + 'NBvAM/Negotiation Level:' + @NewLineChar + trim(isnull(NB_v_AM_Negotiation_level__c, ''))
		)
		, iif(len(trim(isnull(What_else_3__c	, ''))) = 0, ''
			, @DoubleNewLine + 'What else can you tell me:' + @NewLineChar + trim(isnull(What_else_3__c	, ''))
		)
		, iif(len(trim(isnull(RFL_3__c, ''))) = 0, ''
			, @DoubleNewLine + 'RFL: ' + @NewLineChar + trim(isnull(RFL_3__c, ''))
		)
	)
) as [candidate-company3]
, trim(isnull([Company_Name_3__c], '')) as [candidate-employer3]
, trim(isnull([Job_Title_3__c], '')) as [candidate-jobTitle3]
, iif(ISDATE(Date_joined_3__c) = 1, trim(isnull(convert(varchar(50), cast(Date_joined_3__c as datetime), 111), '')), '') as [candidate-startDate3]
, trim(isnull(convert(varchar(50), cast(x.[Date_employment_ended_3__c] as datetime), 111), '')) as [candidate-endDate3]

--, db-field-not-found as [candidate-keyword]

--, replace(trim(isnull(x.AVTRRT__AutoPopulate_Skillset__c, '')), ';', ',') as [candidate-skills]

--, db-field-not-found as [candidate-numberOfEmployers]

, concat(
	'Job #1' + @DoubleNewLine
	, trim(', ' from
		concat(
			iif(len(trim(isnull(Main_Competitors__c, ''))) = 0, ''
				, 'Main Competitors: ' + @NewLineChar + trim(isnull(Main_Competitors__c, ''))
			)
			, iif(len(trim(isnull(What_are_they_selling__c, ''))) = 0, ''
				, @DoubleNewLine + 'What are they selling: ' + @NewLineChar + trim(isnull(What_are_they_selling__c, ''))
			)
			, iif(len(trim(isnull(Sales_Targets__c, ''))) = 0, ''
				, @DoubleNewLine + 'Sales Targets:' + @NewLineChar + trim(isnull(Sales_Targets__c, ''))
			)
			, iif(len(trim(isnull(Example_of_3_deals__c, ''))) = 0, ''
				, @DoubleNewLine + 'Example of 3 deals:' + @NewLineChar + trim(isnull(Example_of_3_deals__c, ''))
			)
			, iif(len(trim(isnull(Prizes_Achievements__c, ''))) = 0, ''
				, @DoubleNewLine + 'Prizes, Achievements:' + @NewLineChar + trim(isnull(Prizes_Achievements__c, ''))
			)
			, iif(len(trim(isnull(What_are_they_selling__c, ''))) = 0, ''
				, @DoubleNewLine + 'Basic Salary + OTE + Bens:' + @NewLineChar + trim(isnull(What_are_they_selling__c, ''))
			)
			, iif(len(trim(isnull(Reports_to_1__c, ''))) = 0, ''
				, @DoubleNewLine + 'Reports to:' + @NewLineChar + trim(isnull(Reports_to_1__c, ''))
			)
			, iif(len(trim(isnull(Languages__c, ''))) = 0, ''
				, @DoubleNewLine + 'Languages:' + @NewLineChar + trim(isnull(Languages__c, ''))
			)
			, iif(len(trim(isnull(Who_are_they_selling_to__c, ''))) = 0, ''
				, @DoubleNewLine + 'Who are they selling to:' + @NewLineChar + trim(isnull(Who_are_they_selling_to__c, ''))
			)
			, iif(len(trim(isnull(Order_values_Sales_cycles_LoN__c, ''))) = 0, ''
				, @DoubleNewLine + 'Order values/Sales cycles:' + @NewLineChar + trim(isnull(Order_values_Sales_cycles_LoN__c, ''))
			)
			, iif(len(trim(isnull(NBvAM_Negotiation_Level__c, ''))) = 0, ''
				, @DoubleNewLine + 'NBvAM/Negotiation Level:' + @NewLineChar + trim(isnull(NBvAM_Negotiation_Level__c, ''))
			)
			, iif(len(trim(isnull(What_else_can_you_tell_me__c	, ''))) = 0, ''
				, @DoubleNewLine + 'What else can you tell me:' + @NewLineChar + trim(isnull(What_else_can_you_tell_me__c	, ''))
			)
			, iif(len(trim(isnull(End_of_Financial_Year_CONTACT__c, ''))) = 0, ''
				, @DoubleNewLine + 'End of Financial Year:' + @NewLineChar + trim(isnull(End_of_Financial_Year_CONTACT__c, ''))
			)
			, iif(len(trim(isnull(RFL__c, ''))) = 0, ''
				, @DoubleNewLine + 'RFL: ' + @NewLineChar + trim(isnull(RFL__c, ''))
			)
			, iif(len(trim(isnull(What_earnt_in_last_3_years__c, ''))) = 0, ''
				, @DoubleNewLine + 'What earnt in last 3 years:' + @NewLineChar + trim(isnull(What_earnt_in_last_3_years__c, ''))
			)
		)
	)
	, @DoubleNewLine + 'Job #2' + @DoubleNewLine
	, trim(', ' from
		concat(
			iif(len(trim(isnull(What_are_they_selling_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'What are they selling: ' + @NewLineChar + trim(isnull(What_are_they_selling_2__c, ''))
			)
			, iif(len(trim(isnull(Sales_targets_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Sales Targets:' + @NewLineChar + trim(isnull(Sales_targets_2__c, ''))
			)
			, iif(len(trim(isnull(Example_of_3_deals_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Example of 3 deals:' + @NewLineChar + trim(isnull(Example_of_3_deals_2__c, ''))
			)
			, iif(len(trim(isnull(Basic_Salary_ote_bens_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Prizes, Achievements:' + @NewLineChar + trim(isnull(Basic_Salary_ote_bens_2__c, ''))
			)
			, iif(len(trim(isnull(Prizes_Achievements_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Basic Salary + OTE + Bens:' + @NewLineChar + trim(isnull(Prizes_Achievements_2__c, ''))
			)
			, iif(len(trim(isnull(Reports_to_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Reports to:' + @NewLineChar + trim(isnull(Reports_to_2__c, ''))
			)
			, iif(len(trim(isnull(Who_are_they_selling_to_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Who are they selling to:' + @NewLineChar + trim(isnull(Who_are_they_selling_to_2__c, ''))
			)
			, iif(len(trim(isnull(Order_values_sales_cycles_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'Order values/Sales cycles:' + @NewLineChar + trim(isnull(Order_values_sales_cycles_2__c, ''))
			)
			, iif(len(trim(isnull(NB_v_AM_Negotiation_level_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'NBvAM/Negotiation Level:' + @NewLineChar + trim(isnull(NB_v_AM_Negotiation_level_2__c, ''))
			)
			, iif(len(trim(isnull(What_else_can_you_tell_me_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'What else can you tell me:' + @NewLineChar + trim(isnull(What_else_can_you_tell_me_2__c, ''))
			)
			, iif(len(trim(isnull(RFL_2__c, ''))) = 0, ''
				, @DoubleNewLine + 'RFL: ' + @NewLineChar + trim(isnull(RFL_2__c, ''))
			)
		)
	)
	, @DoubleNewLine + 'Job #3' + @DoubleNewLine
	, trim(', ' from
		concat(
			iif(len(trim(isnull(What_are_they_selling_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'What are they selling: ' + @NewLineChar + trim(isnull(What_are_they_selling_3__c, ''))
			)
			, iif(len(trim(isnull(Sales_targets_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Sales Targets:' + @NewLineChar + trim(isnull(Sales_targets_3__c, ''))
			)
			, iif(len(trim(isnull(Example_of_deals_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Example of 3 deals:' + @NewLineChar + trim(isnull(Example_of_deals_3__c, ''))
			)
			, iif(len(trim(isnull(Prizes_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Prizes, Achievements:' + @NewLineChar + trim(isnull(Prizes_3__c, ''))
			)
			, iif(len(trim(isnull(Basic_salary_ote_bens_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Basic Salary + OTE + Bens:' + @NewLineChar + trim(isnull(Basic_salary_ote_bens_3__c, ''))
			)
			, iif(len(trim(isnull(Who_report_to_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Reports to:' + @NewLineChar + trim(isnull(Who_report_to_3__c, ''))
			)
			, iif(len(trim(isnull(What_are_they_selling_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Who are they selling to:' + @NewLineChar + trim(isnull(What_are_they_selling_3__c, ''))
			)
			, iif(len(trim(isnull(Sales_cycles_Order_values_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'Order values/Sales cycles:' + @NewLineChar + trim(isnull(Sales_cycles_Order_values_3__c, ''))
			)
			, iif(len(trim(isnull(NB_v_AM_Negotiation_level__c, ''))) = 0, ''
				, @DoubleNewLine + 'NBvAM/Negotiation Level:' + @NewLineChar + trim(isnull(NB_v_AM_Negotiation_level__c, ''))
			)
			, iif(len(trim(isnull(What_else_3__c	, ''))) = 0, ''
				, @DoubleNewLine + 'What else can you tell me:' + @NewLineChar + trim(isnull(What_else_3__c	, ''))
			)
			, iif(len(trim(isnull(RFL_3__c, ''))) = 0, ''
				, @DoubleNewLine + 'RFL: ' + @NewLineChar + trim(isnull(RFL_3__c, ''))
			)
		)
	)
	, @DoubleNewLine + 'Previous job History + notes' + @DoubleNewLine
	, iif(len(trim(isnull(Company_4__c, ''))) = 0, ''
		, @DoubleNewLine + 'Company 4 Name, Position, Dates, Basic: ' + @NewLineChar + trim(isnull(Company_4__c, ''))
	)
	, iif(len(trim(isnull(Company_5_Name_Position_Dates_Basic__c, ''))) = 0, ''
		, @DoubleNewLine + 'Company 5 Name, Position, Dates, Basic: ' + @NewLineChar + trim(isnull(Company_5_Name_Position_Dates_Basic__c, ''))
	)
	, iif(len(trim(isnull(Compamy_6_Name_Dates_Position_Basic__c, ''))) = 0, ''
		, @DoubleNewLine + 'Company 6 Name, Position, Dates, Basic: ' + @NewLineChar + trim(isnull(Compamy_6_Name_Dates_Position_Basic__c, ''))
	)
	, iif(len(trim(isnull(Info__c, ''))) = 0, ''
		, @DoubleNewLine + 'Info: ' + @NewLineChar + trim(isnull(Info__c, ''))
	)
)
as [candidate-workHistory]

--, db-field-not-found as [candidate-comments]

, trim(@NewLineChar from
	'External ID: ' + x.Id
	+ iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Description:' + @NewLineChar + trim(isnull(x.Description, '')), '')
	+ @DoubleNewLine + 'Job Requirements' + @NewLineChar + '----------------' + @NewLineChar
	--+ iif(len(trim(isnull(x.Marital_Status__c, ''))) > 0, @NewLineChar + 'Marital Status: ' + trim(isnull(Marital_Status__c, '')), '')
	--+ iif(len(trim(isnull(cast(x.Date_of_Consent__c as varchar(50)), ''))) > 0, @NewLineChar + 'Date of Consent: ' + trim(isnull(cast(x.Date_of_Consent__c as varchar(50)), '')), '')
	--+ iif(len(trim(isnull(x.Driving_Licence_points__c, ''))) > 0, @NewLineChar + 'Driving Licence points: ' + trim(isnull(x.Driving_Licence_points__c, '')), '')
	+ iif(len(trim(isnull(x.Role__c, ''))) > 0, @NewLineChar + 'Role: ' + trim(isnull(x.Role__c, '')), '')
	+ iif(len(trim(isnull(x.Location_Travel__c, ''))) > 0, @NewLineChar + 'Location/Travel: ' + trim(isnull(x.Location_Travel__c, '')), '')
	+ iif(len(trim(isnull(x.Minimum_Salary_needed__c, ''))) > 0, @NewLineChar + 'Minimum Salary needed: ' + trim(isnull(x.Minimum_Salary_needed__c, '')), '')
	+ iif(len(trim(isnull(x.What_else__c	, ''))) > 0, @NewLineChar + 'What else: ' + trim(isnull(x.What_else__c	, '')), '')
	+ iif(len(trim(isnull(x.Companies_not_initerested_in__c, ''))) > 0, @NewLineChar + 'Companies not initerested in: ' + trim(isnull(x.Companies_not_initerested_in__c, '')), '')
	+ iif(len(trim(isnull(x.Interview_availability__c, ''))) > 0, @NewLineChar + 'Interview/start date availability+notice: ' + trim(isnull(x.Interview_availability__c, '')), '')
	+ iif(len(trim(isnull(x.Type_of_company__c, ''))) > 0, @NewLineChar + 'Type of company: ' + trim(isnull(x.Type_of_company__c, '')), '')
	+ iif(len(trim(isnull(x.What_selling__c	, ''))) > 0, @NewLineChar + 'What selling: ' + trim(isnull(x.What_selling__c	, '')), '')
	+ iif(len(trim(isnull(x.Who_selling_to__c, ''))) > 0, @NewLineChar + 'Who selling to: ' + trim(isnull(x.Who_selling_to__c, '')), '')
	+ iif(len(trim(isnull(x.MIT__c, ''))) > 0, @NewLineChar + 'MIT: ' + trim(isnull(x.MIT__c, '')), '')
	+ iif(len(trim(isnull(x.Compaines_like_to_work_for__c, ''))) > 0, @NewLineChar + 'Companies like to work for: ' + trim(isnull(x.Compaines_like_to_work_for__c, '')), '')
	+ iif(len(trim(isnull(x.What_else_has_the_candidate_on_the_go_be__c	, ''))) > 0, @NewLineChar + 'What else has the candidate on the go/be: ' + trim(isnull(x.What_else_has_the_candidate_on_the_go_be__c	, '')), '')
	--+ iif(len(trim(isnull(x.LastModifiedById, ''))) > 0 and u.Id is not null
	--	, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
	--	, '')
)
as [candidate-note]

--, db-field-not-found as [candidate-photo]

, isnull(cd.Docs, '') as [candidate-resume]

, u.Username as [candidate-owners]
, cast(x.CreatedDate as datetime) as CreatedDate

into [dbo].[VCCandidates]

from
VCCanIdxs cis -- 17476
left join Contact x on cis.Id = x.Id
left join VCCanDocs cd on x.Id = cd.ContactId
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
order by cast(x.CreatedDate as datetime)
--and cd.Docs is not null
--order by x.CreatedDate
--) abc
--where len(trim(isnull(abx.[candidate-Country], ''))) = 0
--where len(isnull(abx.[candidate-resume], '')) > 0
--select 9049 + 8427 -- => 17476

--select count(*) from Contact

--select * from RecordType
--where id = '012b0000000J2RD'
--Id	Name
--012b0000000J2RDAA0	Candidate
--012b0000000J2REAA0	Contact

--declare @PageSize int = 10
--declare @PageNumber int = 1

--SELECT
--	Id
--	, AVTRRT__Previous_Employers__c
--	, AVTRRT__Previous_Titles__c
--	, AVTRRT__Skill_Matched__c
--	, AVTRRT__Current_Employer__c
--	, AVTRRT__Current_Pay__c
--	, CreatedDate
--  FROM dbo.Contact
--  where
--  len(trim(isnull(AVTRRT__Previous_Employers__c, ''))) > 0
--  --len(trim(isnull(AVTRRT__Skill_Matched__c, ''))) > 0
--  ORDER BY CreatedDate 
--  OFFSET @PageSize * (@PageNumber - 1) ROWS
--  FETCH NEXT @PageSize ROWS ONLY;

--  select top 3
--  (select value from string_split(trim(isnull(AVTRRT__Previous_Employers__c, '')), ',')) as abc
--  from Contact
--  where len(trim(isnull(AVTRRT__Previous_Employers__c, ''))) > 0

--select a.Id as AttachmentId, x.Id as ContentVersionId, a.Name, x.PathOnClient
--from [Attachment] a
--left join [ContentVersion] c on a.Name = x.PathOnClient
--where x.Id is not null
---- 24354
--Order by x.PathOnClient

select * from [dbo].[VCCandidates]
order by CreatedDate
-- the paging comes here
--OFFSET     14400 ROWS       -- skip N rows
--FETCH NEXT 3600 ROWS ONLY; -- take M rows

--select y.* from [VCCandidates] x
--join [VCContacts] y on x.[candidate-externalId] = y.[contact-externalId]