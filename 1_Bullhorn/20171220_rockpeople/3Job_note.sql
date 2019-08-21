with
 note as (
        select jobPostingID
	, Stuff(  Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
+ Coalesce('Assignment Type: ' + NULLIF(cast(a.customText11 as varchar(max)), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(cast(a.status as varchar(max)), '') + char(10), '')
+ Coalesce('Priority: ' + NULLIF(cast(a.type as varchar(max)), '') + char(10), '')
+ Coalesce('Reporting to (Contact): ' + NULLIF(cast(a.reportToUserID as varchar(max)), '') + char(10), '')
+ Coalesce('Reporting to (Job Title): ' + NULLIF(cast(a.reportTo as varchar(max)), '') + char(10), '')
+ Coalesce('Source: ' + NULLIF(cast(a.source as varchar(max)), '') + char(10), '')
+ Coalesce('Superannuation Rate: ' + NULLIF(cast(a.customInt1 as varchar(max)), '') + char(10), '')
+ Coalesce('Benefits: ' + NULLIF(cast(a.benefits as varchar(max)), '') + char(10), '')
+ Coalesce('Benefit Details: ' + NULLIF(cast(a.customTextBlock2 as varchar(max)), '') + char(10), '')
+ Coalesce('Perm Fee (%): ' + NULLIF(cast(a.feeArrangement as varchar(max)), '') + char(10), '')
+ Coalesce('PO Number: ' + NULLIF(cast(a.customText3 as varchar(max)), '') + char(10), '')
+ Coalesce('# required: ' + NULLIF(cast(a.numOpenings as varchar(max)), '') + char(10), '')
+ Coalesce('Skills / Experience: ' + NULLIF(cast(a.skillsInfoHeader as varchar(max)), '') + char(10), '')
+ Coalesce('Preferred Work Experience: ' + NULLIF(cast(a.customText4 as varchar(max)), '') + char(10), '')
+ Coalesce('Minimum Experience: ' + NULLIF(cast(a.yearsRequired as varchar(max)), '') + char(10), '')
+ Coalesce('Education Requirements: ' + NULLIF(cast(a.educationDegree as varchar(max)), '') + char(10), '')
+ Coalesce('Job Duration: ' + NULLIF(cast(a.durationWeeks as varchar(max)), '') + char(10), '')
+ Coalesce('Pay Rate: ' + NULLIF(cast(a.payRate as varchar(max)), '') + char(10), '')
+ Coalesce('Scheduled End: ' + NULLIF(cast(a.dateEnd as varchar(max)), '') + char(10), '')
+ Coalesce('Pay Rate: ' + NULLIF(cast(a.salaryUnit as varchar(max)), '') + char(10), '')
+ Coalesce('Client Bill Rate: ' + NULLIF(cast(a.clientBillRate as varchar(max)), '') + char(10), '')
+ Coalesce('Days per week: ' + NULLIF(cast(a.hoursPerWeek as varchar(max)), '') + char(10), '')
+ Coalesce('Address: ' + NULLIF(cast(a.address as varchar(max)), '') + char(10), '')
+ Coalesce('City: ' + NULLIF(cast(a.city as varchar(max)), '') + char(10), '')
+ Coalesce('State: ' + NULLIF(cast(a.state as varchar(max)), '') + char(10), '')
+ Coalesce('Post Code: ' + NULLIF(cast(a.zip as varchar(max)), '') + char(10), '')
+ Coalesce('Country: ' + NULLIF(cast(a.countryID as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as note
        -- select top 50 *
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        left join tmp_country on JP.countryID = tmp_country.CODE
        )
        
select * from note