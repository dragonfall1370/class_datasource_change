drop table if exists [dbo].[VCJobs]

declare @NewLineChar as char(1) = char(10);
declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
declare @jdNewLine as char(2) = char(13) + char(10);

select

jcIdx.ContactId as [position-contactId]

, trim(isnull(x.Id, '')) as [position-externalId]

, idx.JobTitle as [position-title]

, cast(trim(isnull(x.TotalOpportunityQuantity, '0')) as int) as [position-headcount]

--, db-field-not-found as [position-type]

, 'EUR' as [position-currency]

, iif(len(trim(isnull(Amount, ''))) = 0, cast('0.00' as money), cast(trim(isnull(Amount, '')) as money)) as [position-actualSalary]

--, iif(len(trim(isnull(Amount, ''))) = 0, 0.00, cast(trim(isnull(Amount, '')) as money)) as [position-payRate]

--, '' as [position-contractLength]

, trim(isnull(convert(varchar(50), cast(x.Date__c as datetime), 111), '')) as [position-startDate]

, iif(IsClosed is null or IsClosed = 0
	, trim(isnull(convert(varchar(50), dateadd(year, 1, getdate()), 111), ''))
	, trim(isnull(convert(varchar(50), cast(x.CloseDate as datetime), 111), convert(varchar(50), dateadd(day, -7, getdate()), 111)))
) as [position-endDate]

, trim(@NewLineChar from
	'External ID: ' + x.Id
	
	+ @DoubleNewLine + 'Opportunity Detail' + @NewLineChar

	+ iif(len(trim(isnull(x.Type, ''))) > 0, @NewLineChar + 'Type: ' + trim(isnull(x.Type, '')), '')
	
	+ iif(len(trim(isnull(x.LeadSource, ''))) > 0, @NewLineChar + 'Lead Source: ' + trim(isnull(x.LeadSource, '')), '')

	+ iif(len(trim(isnull(x.Job_Number__c, ''))) > 0, @NewLineChar + 'Job Number: ' + trim(isnull(x.Job_Number__c, '')), '')
	
	+ iif(len(trim(isnull(x.Amount, ''))) > 0, @NewLineChar + 'Amount: ' + trim(isnull(x.Amount, '')), '')

	+ iif(len(trim(isnull(x.StageName, ''))) > 0, @NewLineChar + 'Stage: ' + trim(isnull(x.StageName, '')), '')
	
	+ iif(len(trim(isnull(x.Probability, ''))) > 0, @NewLineChar + 'Probability (%): ' + trim(isnull(x.Probability, '')), '')

	+ iif(len(trim(isnull(x.Superior_name_position__c, ''))) > 0, @NewLineChar + 'Superior name + position: ' + trim(isnull(x.Superior_name_position__c, '')), '')

	+ iif(len(trim(isnull(x.Contact_name_position__c, ''))) > 0, @NewLineChar + 'Contact name + position: ' + trim(isnull(x.Contact_name_position__c, '')), '')

) as [position-note]

, replace(replace(
	concat(
		'------------------------------', @NewLineChar, 'Job requirements', @NewLineChar, '------------------------------', @DoubleNewLine
	
		, iif(len(trim(isnull(x.Key_info_for_Breifing_Matching__c, ''))) > 0, @NewLineChar + 'Key info for Breifing/Matching: ' + @NewLineChar + trim(isnull(x.Key_info_for_Breifing_Matching__c, '')), '')
	
		, iif(len(trim(isnull(x.What_personality__c, ''))) > 0, @NewLineChar + 'What personality:' + @NewLineChar + trim(isnull(x.What_personality__c, '')), '')

		, iif(len(trim(isnull(x.What_experience_how_much__c, ''))) > 0, @NewLineChar + 'What experience & how much:' + @NewLineChar + trim(isnull(x.What_experience_how_much__c, '')), '')

		, iif(len(trim(isnull(x.What_else_can_you_tell_me_about_the_role__c, ''))) > 0, @NewLineChar + 'What else can you tell me about the role:' + @NewLineChar + trim(isnull(x.What_else_can_you_tell_me_about_the_role__c, '')), '')

		, @DoubleNewLine, '------------------------------', @NewLineChar, 'Job description', @NewLineChar, '------------------------------', @DoubleNewLine
	
		, iif(len(trim(isnull(x.Selling_what__c, ''))) > 0, @NewLineChar + 'Selling what:' + @NewLineChar + trim(isnull(x.Selling_what__c, '')), '')

		, iif(len(trim(isnull(x.Role_NBvAM__c, ''))) > 0, @NewLineChar + 'Role(NBvAM): ' + trim(isnull(x.Role_NBvAM__c, '')), '')

		, iif(len(trim(isnull(x.Targets__c, ''))) > 0, @NewLineChar + 'Targets: ' + trim(isnull(x.Targets__c, '')), '')

		, iif(len(trim(isnull(x.What_support_do_you_offer__c, ''))) > 0, @NewLineChar + 'What support do you offer:' + @NewLineChar + trim(isnull(x.What_support_do_you_offer__c, '')), '')

		, iif(len(trim(isnull(x.Homeworking__c, ''))) > 0, @NewLineChar + 'Homeworking: ' + trim(isnull(x.Homeworking__c, '')), '')

		, iif(len(trim(isnull(x.Who_selling_to__c, ''))) > 0, @NewLineChar + 'Who selling to:' + @NewLineChar + trim(isnull(x.Who_selling_to__c, '')), '')

		, iif(len(trim(isnull(x.Level_of_Negotiation__c, ''))) > 0, @NewLineChar + 'Level of Negotiation: ' + trim(isnull(x.Level_of_Negotiation__c, '')), '')

		, iif(len(trim(isnull(x.Sales_cycles_AoV_Number_of_deals__c, ''))) > 0, @NewLineChar + 'Sales cycles/AoV/Number of deals: ' + trim(isnull(x.Sales_cycles_AoV_Number_of_deals__c, '')), '')

		, iif(len(trim(isnull(x.Based_from_which_office__c, ''))) > 0, @NewLineChar + 'Based from which office: ' + trim(isnull(x.Based_from_which_office__c, '')), '')

		, iif(len(trim(isnull(x.Geographical_area_covering__c, ''))) > 0, @NewLineChar + 'Georaphical area covering: ' + trim(isnull(x.Geographical_area_covering__c, '')), '')

		, @DoubleNewLine, '------------------------------', @NewLineChar, 'Salary package', @NewLineChar, '------------------------------', @DoubleNewLine

		, iif(len(trim(isnull(x.Basic_ote__c, ''))) > 0, @NewLineChar + 'Basic + ote: ' + trim(isnull(x.Basic_ote__c, '')), '')

		, iif(len(trim(isnull(x.Additional_Benefits__c, ''))) > 0, @NewLineChar + 'Additional Benefits: ' + trim(isnull(x.Additional_Benefits__c, '')), '')

		, iif(len(trim(isnull(x.Car_car_allowance__c, ''))) > 0, @NewLineChar + 'Car/car allowance: ' + trim(isnull(x.Car_car_allowance__c, '')), '')

		, iif(len(trim(isnull(x.salary_notes_gfuarantee__c, ''))) > 0, @NewLineChar + 'Salary notes / guarantee: ' + trim(isnull(x.salary_notes_gfuarantee__c, '')), '')

		, @DoubleNewLine, '------------------------------', @NewLineChar, 'Additional Information', @NewLineChar, '------------------------------', @DoubleNewLine

		, iif(len(trim(isnull(x.Why_work_for__c, ''))) > 0, @NewLineChar + 'Why work for:' + @NewLineChar + trim(isnull(x.Why_work_for__c, '')), '')

		, iif(len(trim(isnull(x.Client_background__c, ''))) > 0, @NewLineChar + 'Client background: ' + trim(isnull(x.Client_background__c, '')), '')

		, iif(len(trim(isnull(x.Reports_to__c, ''))) > 0, @NewLineChar + 'Reports to: ' + trim(isnull(x.Reports_to__c, '')), '')

		, iif(len(trim(isnull(x.what_is_interview_procedure__c, ''))) > 0, @NewLineChar + 'What is interview procedure: ' + trim(isnull(x.what_is_interview_procedure__c, '')), '')

		, iif(len(trim(isnull(x.Who_else_recruiting_in_your_organisation__c, ''))) > 0, @NewLineChar + 'Who else recruiting in your organisation: ' + trim(isnull(x.Who_else_recruiting_in_your_organisation__c, '')), '')

		, iif(len(trim(isnull(x.Interview_dates__c, ''))) > 0, @NewLineChar + 'Interview dates: ' + trim(isnull(x.Interview_dates__c, '')), '')

		, iif(len(trim(isnull(x.call_back_time__c, ''))) > 0, @NewLineChar + 'Call back time: ' + trim(isnull(x.call_back_time__c, '')), '')

		, iif(len(trim(isnull(x.Terms__c, ''))) > 0, @NewLineChar + 'Terms: ' + trim(isnull(x.Terms__c, '')), '')

		, iif(len(trim(isnull(x.Comments__c, ''))) > 0, @NewLineChar + 'Comments:' + @NewLineChar + trim(isnull(x.Comments__c, '')), '')

		, iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Description:' + @NewLineChar + trim(isnull(x.Description, '')), '')

		, iif(len(trim(isnull(x.Background_info_year_est_culture_turnove__c, ''))) > 0, @NewLineChar + 'Background info(year est,culture,turnover): ' + trim(isnull(x.Background_info_year_est_culture_turnove__c, '')), '')

		, iif(len(trim(isnull(x.Competition_clients_partners__c, ''))) > 0, @NewLineChar + 'Competition/clients/partners: ' + trim(isnull(x.Competition_clients_partners__c, '')), '')

		, iif(len(trim(isnull(x.Reason_for_vacancy__c, ''))) > 0, @NewLineChar + 'Reason for vacancy: ' + trim(isnull(x.Reason_for_vacancy__c, '')), '')

		, iif(len(trim(isnull(x.End_of_Financial_Year__c, ''))) > 0, @NewLineChar + 'End of Financial Year: ' + trim(isnull(x.End_of_Financial_Year__c, '')), '')

		, iif(len(trim(isnull(x.What_other_measures_have_you_taken_to_re__c, ''))) > 0, @NewLineChar + 'What other measures have you taken to re:' + @NewLineChar + trim(isnull(x.What_other_measures_have_you_taken_to_re__c, '')), '')

		, iif(len(trim(isnull(x.how_many_more_staff_recruit_in_3_months__c, ''))) > 0, @NewLineChar + 'How many more staff recruit in 3 months: ' + trim(isnull(x.how_many_more_staff_recruit_in_3_months__c, '')), '')

		, iif(len(trim(isnull(x.DM_s_Management_Style__c, ''))) > 0, @NewLineChar + 'DM''s Management Style: ' + trim(isnull(x.DM_s_Management_Style__c, '')), '')

		, iif(len(trim(isnull(x.What_are_your_drivers_ambitions__c, ''))) > 0, @NewLineChar + 'What are your drivers/ambitions: ' + trim(isnull(x.What_are_your_drivers_ambitions__c, '')), '')
	)
	, char(13), ''), char(10), '<br/>'
) as [position-publicDescription]

--, trim(isnull(x.AVTRRT__Job_Summary__c, '')) as [position-internalDescription]

, d.Docs as [position-document]

--, db-field-not-found as [position-otherDocument]

, u.Username as [position-owners]

, cast(x.CreatedDate as datetime) as CreatedDate

into [dbo].[VCJobs]

from
[VCJobIdxs] idx
left join VCJobContactIdxs jcIdx on idx.Id = jcIdx.JobId
left join Opportunity x on idx.Id = x.Id
left join [User] u on x.OwnerId = u.Id
left join [VCJobDocs] d on x.Id = d.JobId
order by cast(x.CreatedDate as datetime)

select * from [dbo].[VCJobs]
order by CreatedDate