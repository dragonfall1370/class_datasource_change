SET ANSI_PADDING ON

GO


declare @LineDouble as char(2) = char(10) + char(10);

drop table if exists VCComs;

with note as (
	
	select CC.clientCorporationID

	, trim(@LineDouble from
		concat(
			concat(@LineDouble, 'Client Corporation ID: ', CC.clientCorporationID)

			--, concat(@LineDouble, 'Billing Phone: ', CC.billingPhone)

			--+ iif(r.clientCorporationID is null, '',
			--	 concat(
			--		@LineDouble
			--		, concat(replicate('-', len('Stats')*2), char(10), 'Stats', char(10), replicate('-', len('Stats')*2), char(10))
			--		, concat('CorpName: ', r.CorpName, char(10))
			--		, concat('Fill Ratio: ', r.FillRatio, char(10))
			--		, concat('Fill Ratio Year: ', r.FillRatio_Year, char(10))
			--		, concat('InterViews: ', r.InterViews, char(10))
			--		, concat('InterViews Year: ', r.InterViews_Year, char(10))
			--		, concat('Interview to Placement Ratio: ', r.InterviewtoPlacement_Ratio, char(10))
			--		, concat('Num Jobs: ', r.NumJobs, char(10))
			--		, concat('Num Jobs Year: ', r.NumJobs_Year, char(10))
			--		, concat('Placements: ', r.Placements, char(10))
			--		, concat('Placements Year: ', r.Placements_Year, char(10))
			--		, concat('Submissions: ', r.Submissions, char(10))
			--		, concat('Submissions Year: ', r.Submissions_Year, char(10))
			--		, concat('Subto Interview Ratio: ', r.SubtoInterview_Ratio, char(10))
			--		, concat('YTD Interview to Placement Ratio: ', r.YTD_InterviewtoPlacement_Ratio, char(10))
			--		, concat('YTD Sub to Interview Ratio: ', r.YTD_SubtoInterview_Ratio, char(10))
			--		, replicate('-', len('Stats')*2)
			--	)
			--)

		, concat(@LineDouble, 'Company Description: ', @LineDouble, [dbo].[udf_StripHTML](trim(isnull(convert(nvarchar(max), CC.companyDescription), ''))))

		, concat(@LineDouble, 'Contact: ', cc.customText1)

		, concat(@LineDouble, 'Brand: ', cc.customText3)

		, concat(@LineDouble, 'General Comments:', @LineDouble, cast(CC.notes as varchar(max)))

		--, concat(@LineDouble, 'Date Added: ', CC.dateAdded)

		--, concat(@LineDouble, 'Date Last Modified: ', v.DateLastModified)

		--, concat(@LineDouble, 'VAT Nr: ', CC.fax)

		--, concat(@LineDouble, 'Standard Fee Arrangement (%): ', CC.feeArrangement)

		--, concat(@LineDouble, 'Invoice Format Information: ', CC.invoiceFormat)

		--, concat(@LineDouble, '# of Offices: ', CC.numOffices)

		--, concat(@LineDouble, 'Parent Company: ', CC.parentClientCorporationID)

		--, concat(@LineDouble, 'Revenue: ', CC.revenue)

		--, concat(@LineDouble, 'Status: ', CC.status)

		--, concat(@LineDouble, 'Tax %: ', CC.taxRate)

		--+ Coalesce('Opportunities: ' + NULLIF(cast(CC.opportunityTable as varchar(max)), '') + char(10), '')


		
		--+ Coalesce('Expected Close Date: ' + NULLIF(convert(varchar(10),CC.customdate1,120), '') + char(10), '')
		--+ Coalesce('Target Spread: ' + NULLIF(cast(customFloat3 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Expected # Jobs: ' + NULLIF(cast(CC.customInt1 as varchar(max)), '') + char(10), '')
		
		--+ Coalesce('Ltd Co VAT No: ' + NULLIF(customText12, '') + char(10), '')
		--+ Coalesce('Bank Account Name: ' + NULLIF(CC.customText14, '') + char(10), '')
		--+ Coalesce('Bank Account Number: ' + NULLIF(CC.customText15, '') + char(10), '')
		--+ Coalesce('Bank Sort Code: ' + NULLIF(CC.customText16, '') + char(10), '')
		--+ Coalesce('Self Bill?: ' + NULLIF(customText17, '') + char(10), '')
		--+ Coalesce('IBAN: ' + NULLIF(customText18, '') + char(10), '')
		--+ Coalesce('Ltd Co Number: ' + NULLIF(CC.customText2, '') + char(10), '')
		
		--+ Coalesce('PipeLine Status: ' + NULLIF(customText4, '') + char(10), '')
		--+ Coalesce('BIC: ' + NULLIF(CC.customText5, '') + char(10), '')
		--+ Coalesce('Typical Bonus Plan: ' + NULLIF(cast(CC.customTextBlock1 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Email address for remmittance: ' + NULLIF(cast(CC.customTextBlock3 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Contact: ' + NULLIF(cast(CC.billingContact as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Frequency: ' + NULLIF(cast(CC.billingFrequency as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Address 1: ' + NULLIF(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Address 2: ' + NULLIF(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing City: ' + NULLIF(cast(CC.billingCity as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing State: ' + NULLIF(cast(CC.billingState as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Zip: ' + NULLIF(cast(CC.billingZip as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Country: ' + NULLIF(tc.country, '') + char(10), '')
		--+ Coalesce('Ownership: ' + NULLIF(cast(CC.ownership as varchar(max)), '') + char(10), '')
		--+ Coalesce('Standard Fee Arrangement: ' + NULLIF(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
		--+ Coalesce('# of Employees: ' + NULLIF(cast(CC.numEmployees as varchar(max)), '') + char(10), '')
		--+ Coalesce('Fax: ' + NULLIF(cast(CC.fax as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Address: ' + NULLIF(cast(CC.fullBillingAddress as varchar(max)), '') + char(10), '')
		--+ Coalesce('Invoice Format: ' + NULLIF(cast(CC.invoiceFormat as varchar(max)), '') + char(10), '')
		--+ Coalesce('System Date Added: ' + NULLIF(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
		--+ Coalesce('Year Founded: ' + NULLIF(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
		--+ Coalesce('Industry: ' + NULLIF(cast(CC.industryList as varchar(max)), '') + char(10), '')                      
		--+ Coalesce('Business Sector: ' + NULLIF(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
		--+ Coalesce('Ownership: ' + NULLIF(CC.ownership, '') + char(10), '')
		--+ Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '')
		--+ Coalesce('Twitter: ' + NULLIF(CC.twitterHandle, '') + char(10), '')
		--+ Coalesce('Facebook: ' + NULLIF(CC.facebookProfileName, '') + char(10), '')
		--+ Coalesce('LinkedIn: ' + NULLIF(CC.linkedinProfileName, '') + char(10), '')
		--+ Coalesce('Culture: ' + NULLIF(cast(CC.culture as varchar(max)), '') + char(10), '')
		--+ Coalesce('Parent Company: ' + NULLIF(cast(pc.name as varchar(max)), '') + char(10), '') --parentClientCorporationID
		--+ Coalesce('Ownership: ' + NULLIF(cast(CC.Ownership as varchar(max)), '') + char(10), '')
		--+ Coalesce('Status: ' + NULLIF(cast(CC.status as varchar(max)), '') + char(10), '')
		--+ Coalesce('Permanent Fee Structure: ' + NULLIF(cast(customTextBlock4 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Rebate Terms: ' + NULLIF(cast(customTextBlock5 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Monthly Internship Fee (): ' + NULLIF(cast(customFloat1 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Internship Fee Deductible: ' + NULLIF(customText5, '') + char(10), '')
		--+ Coalesce('Month definition: ' + NULLIF(customText10, '') + char(10), '')
		--+ Coalesce('Billing Contact: ' + NULLIF(billingContact, '') + char(10), '')
		--+ Coalesce('Main Location Info: ' + NULLIF(CC.customHeader1, '') + char(10), '')
		--+ Coalesce('Region: ' + NULLIF(cast(CC.customTextBlock2 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Info: ' + NULLIF(CC.customHeader2, '') + char(10), '')
		--+ Coalesce('Billing Contact: ' + NULLIF(cast(CC.billingContact as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Phone: ' + NULLIF(CC.customText5, '') + char(10), '')
		--+ Coalesce('Billing Address 1: ' + NULLIF(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Address 2: ' + NULLIF(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing City: ' + NULLIF(cast(CC.billingCity as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing ZIP: ' + NULLIF(cast(CC.billingZIP as varchar(max)), '') + char(10), '')
		--+ Coalesce('Competitors: ' + NULLIF(cast(CC.competitors as varchar(max)), '') + char(10), '')
		--+ Coalesce('Billing Country: ' + NULLIF(tc.country, '') + char(10), '')
		)
	) as note
                -- select  top 10 * -- select companyDescription -- select *
	from bullhorn1.BH_ClientCorporation CC --where CC.clientCorporationID = 255
	left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) pc on pc.clientCorporationID = CC.clientCorporationID
	left join bullhorn1.View_ClientCorporationLastModified v on v.ClientCorporationID = CC.clientCorporationID
	left join bullhorn1.BH_ClientCorporationRatios r on r.clientCorporationID = CC.clientCorporationID
	--left join VC_Countries tc ON CC.customText11 = tc.code
)
--where CC.customText11 is not null )
-- select top 100 * from bullhorn1.BH_ClientCorporation CC
--select clientCorporationID,[dbo].[fn_ConvertHTMLToText](note) from note where clientCorporationID = 255 --where note like '%&%;%'
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'


-- FILES
, doc (clientCorporationID, ResumeId) as (
	SELECT clientCorporationID
	, STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
	FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID
)
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc


, dup as (
	SELECT clientCorporationID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC
) --where name like 'Azurance'

, headquarter as (
	select distinct parentClientCorporationID,h.name from bullhorn1.BH_ClientCorporation c
	left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
	where parentClientCorporationID is not null and parentClientCorporationID <> ''
)
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)


select
	CC.clientCorporationID as 'company-externalId'
	
	, iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn), iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	
	--, headquarter.name as 'company-headquarter'
	
	, ltrim(
		Stuff(
			Coalesce('  ' + NULLIF(CC.address1, ''), '')
			+ Coalesce(', ' + NULLIF(CC.address2, ''), '')
			+ Coalesce(', ' + NULLIF(CC.city, ''), '')
			+ Coalesce(', ' + NULLIF(CC.state, ''), '')
			+ Coalesce(', ' + NULLIF(tc.country, ''), '')
			, 1, 2, ''
		)
	) as 'company-locationAddress'
	
	, ltrim(
		Stuff(
			Coalesce(', ' + NULLIF(CC.city, ''), '')
			+ Coalesce(', ' + NULLIF(CC.state, ''), '')
			+ Coalesce(', ' + NULLIF(tc.country, ''), '')
			, 1, 2, ''
		)
	) as 'company-locationName'
	
	, isnull(CC.city, '') as 'company-locationCity'
	
	, isnull(CC.state, '') as 'company-locationState'
	
	, isnull(CC.zip, '') as 'company-locationZipCode'
	
	, isnull(tc.abbreviation, 'GB') as 'company-locationCountry'
	
	, [dbo].[ufn_RefinePhoneNumber_V2](isnull(CC.phone, '')) as 'company-phone'
	--, CC.phone as 'company-switchboard'
	--, CC.fax as 'company-fax'
	, [dbo].ufn_RefineWebAddress(isnull(companyURL, '')) as 'company-website' --limitted by 100 characters
	--, CC.ownership as 'company-owners'
	, isnull(doc.ResumeId, '') as 'company-document'
	, [dbo].[fn_ConvertHTMLToText](note.note) as 'company-note'
	--, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
-- select count (*) --560 -- select distinct CC.ownership -- select state

into VCComs

from bullhorn1.BH_ClientCorporation CC
left join VC_Countries tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.ClientCorporationID in (102,153,226,289,656,774,2056,4936)
--where CC.NAME like '%THEQA%'

/* COMMENT - INJECT TO VINCERE
select top 100
        CC.clientCorporationID as 'externalId'
        , cast('-10' as int) as userid
        , dateadded as 'comment_timestamp|insert_timestamp'
        ,Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'comment_content'
from bullhorn1.BH_ClientCorporation CC
where CC.clientCorporationID = '143'
*/

select * from VCComs
--where [company-locationCountry] = 'NULL'