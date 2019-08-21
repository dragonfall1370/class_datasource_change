with companynote as (----1404
	select CC.clientCorporationID,
	       [bullhorn1].[fn_ConvertHTMLToText](
	       Stuff( 
--	               coalesce('BH Company ID: ' + nullif(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')                       
--                        + coalesce('Parent Company: ' + nullif(cast(CC.parentClientCorporationID as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Address 1: ' + nullif(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Address 2: ' + nullif(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing City: ' + nullif(cast(CC.billingCity as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Contact: ' + nullif(cast(CC.billingContact as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Frequency: ' + nullif(cast(CC.billingFrequency as varchar(max)), '') + char(10), '')                      
--                        + coalesce('Billing State: ' + nullif(cast(CC.billingState as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Post Code: ' + nullif(cast(CC.billingZip as varchar(max)), '') + char(10), '')                      
--                        + coalesce('ABN Number: ' + nullif(cast(CC.customText2 as varchar(max)), '') + char(10), '')            
--                        + coalesce('Opportunities: ' + nullif(cast(CC.opportunityTable as varchar(max)), '') + char(10), '')
--                        + coalesce('Ownership: ' + nullif(cast(CC.ownership as varchar(max)), '') + char(10), '')            
--                        + coalesce('System Date Added: ' + nullif(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
--                        + coalesce('Year Founded: ' + nullif(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
--                        + coalesce('Industry: ' + nullif(cast(CC.industryList as varchar(max)), '') + char(10), '')                      
--                        + coalesce('Business Sector: ' + nullif(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
--                        + coalesce('Company Coverage: ' + nullif(CC.customText5, '') + char(10), '')           
--                        + coalesce('Ownership: ' + nullif(CC.ownership, '') + char(10), '')
--                        + coalesce('Company Overview: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](CC.notes), '') + char(10), '')
--                        + coalesce('Twitter: ' + nullif(CC.twitterHandle, '') + char(10), '')
--                        + coalesce('Facebook: ' + nullif(CC.facebookProfileName, '') + char(10), '')
--                        + coalesce('LinkedIn: ' + nullif(CC.linkedinProfileName, '') + char(10), '')
--                        + coalesce('Culture: ' + nullif(cast(CC.culture as varchar(max)), '') + char(10), '')
--                        + coalesce('Ownership: ' + nullif(cast(CC.Ownership as varchar(max)), '') + char(10), '')            
--                        + coalesce('Invoice on: ' + nullif(cc.customText17, '') + char(10), '')
--                        + coalesce('Permanent Fee Structure: ' + nullif(cast(cc.customTextBlock4 as varchar(max)), '') + char(10), '')
--                        + coalesce('Rebate Terms: ' + nullif(cast(cc.customTextBlock5 as varchar(max)), '') + char(10), '')
--                        + coalesce('Monthly Internship Fee (): ' + nullif(cast(cc.customFloat1 as varchar(max)), '') + char(10), '')
--                        + coalesce('Internship Fee Deductible: ' + nullif(cc.customText5, '') + char(10), '')   
--                        + coalesce('Billing Email: ' + nullif(cc.customText2, '') + char(10), '') 
--                        + coalesce('Main Location Info: ' + nullif(CC.customHeader1, '') + char(10), '')
--                        + coalesce('Alternate Phone: ' + nullif(CC.customText14, '') + char(10), '')
--                        + coalesce('Region: ' + nullif(cast(CC.customTextBlock2 as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Info: ' + nullif(CC.customHeader2, '') + char(10), '')
--                        + coalesce('Billing Contact: ' + nullif(cast(CC.billingContact as varchar(max)), '') + char(10), '')                      
--                        + coalesce('Billing Address 1: ' + nullif(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Address 2: ' + nullif(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing City: ' + nullif(cast(CC.billingCity as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing ZIP: ' + nullif(cast(CC.billingZIP as varchar(max)), '') + char(10), '')
--                        + coalesce('SSIC No.: ' + nullif(cast(CC.customInt1 as varchar(max)), '') + char(10), '')
--                        + coalesce('Competitors: ' + nullif(cast(CC.competitors as varchar(max)), '') + char(10), '')
--                        + coalesce('Billing Country: ' + nullif(tc.COUNTRY, '') + char(10), '')
--                        + coalesce('Purchase Orders: ' + nullif(cast(CC.CustomComponent1 as varchar(max)), '') + char(10), '')------------???????             
                        ------MAPPING FILE REQUEST----------
                        coalesce('BH Company ID: ' + nullif(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
                        + coalesce('Billing Contact: ' + nullif(cc.billingContact, '') + char(10), '')
                        + coalesce('Billing Phone: ' + nullif(cast(CC.billingPhone as varchar(max)), '') + char(10), '')
                        + coalesce('Company Description: ' + nullif(cast(CC.companyDescription as varchar(max)), '') + char(10), '')                        
                        + coalesce('Date Fee Agreed: ' + nullif(convert(varchar(10),CC.customdate1,120), '') + char(10), '')
                        + coalesce('Date of TOBs: ' + nullif(convert(varchar(10),CC.customDate2,120), '') + char(10), '')
                        + coalesce('Fee %: ' + nullif(convert(varchar(10),CC.customText1,120), '') + char(10), '')
                        + coalesce('PO Process: ' + nullif(cc.customText10, '') + char(10), '')
                        + coalesce('Right to Supply?: ' + nullif(cc.customText11, '') + char(10), '')
                        + coalesce('Fee Agreement Contact: ' + nullif(CC.customText2, '') + char(10), '')
                        + coalesce('Invoicing Contact 1: ' + nullif(cc.customText3, '') + char(10), '')
                        + coalesce('Invoicing Contact 2: ' + nullif(cast(CC.customText4 as varchar(max)), '') + char(10), '')
--                        + coalesce('Invoicing Contact 3: ' + nullif(CC.customText5, '') + char(10), '')
                        + coalesce('Invoicing Email & instructions: ' + nullif(CC.customText6, '') + char(10), '')
                        + coalesce('Invoicing Email & instructions: ' + nullif(CC.customText7, '') + char(10), '')
--                        + coalesce('Invoicing Email & instructions: ' + nullif(CC.customText8, '') + char(10), '')
                        + coalesce('PO Required?: ' + nullif(CC.customText9, '') + char(10), '')
                        + coalesce('Date Added: ' + nullif(cast(CC.dateAdded as varchar(max)), '') + char(10), '')
                        + coalesce('Date Last Modified: ' + nullif(convert(nvarchar(10),v.DateLastModified,120), '') + char(10), '')
--                        + coalesce('Fax: ' + nullif(cast(CC.fax as varchar(max)), '') + char(10), '')
                        + coalesce('Full Address: '+CONCAT_WS(',',TRIM(',' FROM COALESCE(nullif(cc.address1,''),nullif(cc.address2,''))),nullif(cc.city,''),nullif(cc.state,''),nullif(cc.zip,''),nullif(tc.country,'')) + char(10),'')
                        + coalesce('Full Billing Address: '+CONCAT_WS(',',TRIM(',' FROM COALESCE(nullif(cc.billingAddress1,''),nullif(cc.billingAddress2,''))),nullif(cc.billingCity,''),nullif(cc.billingState,''),nullif(cc.billingZip,''),nullif(tcb.country,'')) + char(10),'')
                        + coalesce('Standard Fee Arrangement %: ' + nullif(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
                        + coalesce('Invoice Format Information: ' + nullif(cast(CC.invoiceFormat as varchar(max)), '') + char(10), '')
                        + coalesce('Company Comments: ' + nullif(cast(CC.notes as varchar(max)), '') + char(10), '')
                        + coalesce('# of Employees: ' + nullif(cast(CC.numEmployees as varchar(max)), '') + char(10), '')
                        + coalesce('# of Offices: ' + nullif(cast(CC.numOffices as varchar(max)), '') + char(10), '')
                        + coalesce('Revenue: ' + nullif(cast(CC.revenue as varchar(max)), '') + char(10), '')
                        + coalesce('Status: ' + nullif(cast(CC.status as varchar(max)), '') + char(10), '')     
                        + coalesce('Tax %: ' + nullif(cast(cc.taxRate as varchar(max)), '') + char(10), '')                  
                        , 1, 0, '')
                ) as note
        from bullhorn1.BH_ClientCorporation CC
--        where CC.clientCorporationID = 255
--        left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) pc on pc.clientCorporationID = CC.clientCorporationID
        left join tmp_country tcb ON CC.countryID = tcb.code
        left join bullhorn1.View_ClientCorporationLastModified v on v.ClientCorporationID = CC.clientCorporationID
        left join dbo.tmp_country tc ON CC.countryID = tc.CODE
) , checkdup as (
        SELECT  clientCorporationID,
                ltrim(rtrim(name)) as name,
                ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn 
	FROM bullhorn1.BH_ClientCorporation CC 
) , headquarter as ( 
        select distinct parentClientCorporationID,h.name 
        from bullhorn1.BH_ClientCorporation c
        left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
        where parentClientCorporationID is not null and parentClientCorporationID <> '' 
) , owner0 as (
        select distinct C.clientCorporationID, C.recruiterUserID, UC.firstName, UC.lastname, [dbo].[fn_RemoveNonASCIIChars](UC.email) as email /*, UC.email2, UC.email3, UC.email_old*/ 
        FROM bullhorn1.BH_Client C 
        left join bullhorn1.BH_UserContact UC on UC.userid = C.recruiterUserID 
        where UC.email is not null---like '%_@_%.__%' /*UC.email is not null and UC.email <> ''*/
) , owner (clientCorporationID,owners) as (
        SELECT clientCorporationID, STRING_AGG( email,',' ) WITHIN GROUP (ORDER BY email) att 
        from owner0 GROUP BY clientCorporationID
) , doc (clientCorporationID,ResumeId) as ( 
        SELECT  clientCorporationID, 
                STRING_AGG(cast(concat(clientCorporationFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY clientCorporationFileID) att 
        from bullhorn1.BH_ClientCorporationFile 
        where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ GROUP BY clientCorporationID
)
select --top 10
         concat('TRCP', CC.clientCorporationID) as 'company-externalId'
        , iif(checkdup.rn > 1,concat(checkdup.name,' ',checkdup.rn), iif(checkdup.name in (null,''),'No CompanyName',checkdup.name)) as 'company-name'
        , headquarter.name as 'company-headquarter'
--        ,CONCAT_WS(',',TRIM(',' FROM COALESCE(nullif(cc.address1,''),nullif(cc.address2,''))),nullif(cc.city,''),nullif(cc.state,''),nullif(cc.zip,''),nullif(tc.country,'')) as 'company-locationAddress'  
--        ,CONCAT_WS(',',TRIM(',' FROM COALESCE(nullif(cc.billingAddress1,''),nullif(cc.billingAddress2,''))),nullif(cc.billingCity,''),nullif(cc.billingState,''),nullif(cc.billingZip,''),nullif(tcb.country,''))  as 'billingcompany-locationAddress'
--        ,CONCAT_WS(',',nullif(cc.city,''),nullif(cc.state,''),nullif(cc.zip,''),nullif(tc.country,'')) as 'company-locationName'  
--        ,CONCAT_WS(',',nullif(cc.billingCity,''),nullif(cc.billingState,''),nullif(cc.billingZip,''),nullif(tcb.country,''))  as 'billingcompany-locationName'  
        , trim(stuff( coalesce(' ' + nullif(CC.address1, ''), '') + coalesce(', ' + nullif(CC.address2, ''), '') 
                        + coalesce(', ' + nullif(CC.city, ''), '') + coalesce(', ' + nullif(CC.state, ''), '') 
                        + coalesce(', ' + nullif(CC.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'company-locationAddress'  
        , trim(stuff( coalesce(', ' + nullif(CC.city, ''), '') + coalesce(', ' + nullif(CC.state, ''), '') 
                        + coalesce(', ' + nullif(CC.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'company-locationName'  
        , CC.city as 'company-locationCity'
        , CC.state as 'company-locationState'
        , CC.zip as 'company-locationZipCode'
        , tc.abbreviation as 'company-locationCountry'
        , CC.phone as 'company-phone'
        , CC.phone as 'company-switchboard'
        , CC.fax as 'company-fax'
        , LEFT(CC.companyURL, 100) as 'company-website' --[limitted by 100 characters]
        , owner.owners as 'company-owners'
        , doc.ResumeId as 'company-document'
        , companynote.note as 'company-note'
--        , convert(varchar(max), cast(companynote.note as binary)) as 'company-note'
--        , coalesce('Company Overview: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](CC.notes), '') + char(10), '') as 'company-comment'
--        , CC.industryList as 'Industry'
--        , CC.numEmployees as 'No. of Employees'
        , CC.dateadded as 'registration date'
from bullhorn1.BH_ClientCorporation CC
left join owner on owner.clientCorporationID = CC.clientCorporationID
left join tmp_country tc ON CC.countryID = tc.code
left join tmp_country tcb ON CC.countryID = tcb.code
left join companynote on CC.clientCorporationID = companynote.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join checkdup on CC.clientCorporationID = checkdup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
where CC.status <> 'Archive' 
--and CC.clientCorporationID=470
UNION ALL
select 'TRCP999999999','Default company','','','','','','','','','','','This is default company from data migration','','','',''
--SELECT  LEFT('https://www.dha.gov.ae/EN/Pages/default.aspx',CHARINDEX('/','https://www.dha.gov.ae/EN/Pages/default.aspx',10)),
--        LEFT('http://www.ksmc.med.sa/user_en/login_ar/index',CHARINDEX('/','http://www.ksmc.med.sa/user_en/login_ar/index',10));