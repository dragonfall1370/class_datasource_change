/*
select top 100 * from SgmtInstances where objectId = '101247-5431-12222'
select top 100 * from segments where segmentId in ('430776-2808-13177','389765-8893-12215','305976-5896-13179') or segment = 'Marketing'
select segment,count(*) from segments group by segment having count(*) > 1
select top 100 * from Contacts;
SELECT contactid, Sector, r1 = ROW_NUMBER() OVER (PARTITION BY contactid ORDER BY contactid desc) from Contacts
select * from Contacts CL where CL.FirstName = 'Alexandra' and CL.LastName = 'Martin'



with segment as ( select si.ObjectId, s.segment from SgmtInstances si left join segments s on s.SegmentId = si.SegmentId where s.segment is not null )
select distinct segment from segment
--select ObjectId from segment group by ObjectId having count(ObjectId) > 1;
--select * from segment where segment = 'Corporate PR'
--select * from segment where objectId in ('396072-2164-11215','881960-4775-15315','191608-4352-9329')



with Segment as (select   c.ContactId, c.FirstName, c.LastName, c.DisplayName
                  , s.segment
           from contacts c
           left join ( select si.ObjectId, s.segment from SgmtInstances si left join segments s on s.SegmentId = si.SegmentId where s.segment is not null) s on s.ObjectId = c.contactid
           where c.descriptor = 2 )
select * from Segment where segment = 'Marketing'
select distinct segment from Segment where lastname = 'Stolliday'
select segment,count(segment) from Segment group by segment
*/



-- CANDIDATE = 2
with fe as (
select
                  distinct CL.ContactId
                --, CL.FirstName, CL.LastName
                , Coalesce(NULLIF(case
                        when Segment.Segment = 'Advertising' then 2984
                        when Segment.Segment = 'Analytics' then 2985
                        when Segment.Segment = 'B2B' then 2986
                        when Segment.Segment = 'Branding and Design' then 2987
                        when Segment.Segment = 'Branding/Design' then 2987
                        when Segment.Segment = 'Broadcasting PR' then 2988
                        when Segment.Segment = 'Business Intelligence' then 2989
                        when Segment.Segment = 'Comms' then 2990
                        when Segment.Segment = 'Consumer' then 2991
                        when Segment.Segment = 'Consumer PR' then 2991
                        when Segment.Segment = 'Content' then 2993
                        when Segment.Segment = 'Corporate' then 2994
                        when Segment.Segment = 'Corporate PR' then 2994
                        when Segment.Segment = 'Cross' then 2995
                        when Segment.Segment = 'CSR' then 2996
                        when Segment.Segment = 'Data Management Platform' then 2997
                        when Segment.Segment = 'Demand Side Platform' then 2998
                        when Segment.Segment = 'Digital' then 2999
                        when Segment.Segment = 'Digital Advertising' then 3000
                        when Segment.Segment = 'Digital Marketing' then 3001
                        when Segment.Segment = 'E-Commerce' then 3002
                        when Segment.Segment = 'Employee Engagement' then 3003
                        when Segment.Segment = 'Events' then 3004
                        when Segment.Segment = 'Film' then 3005
                        when Segment.Segment = 'Finance' then 3006
                        when Segment.Segment = 'financial' then 3007
                        when Segment.Segment = 'Financial PR' then 3007
                        when Segment.Segment = 'Healthcare Advertising' then 3008
                        when Segment.Segment = 'Healthcare PR' then 3009
                        when Segment.Segment = 'HR' then 3010
                        when Segment.Segment = 'In-House Communications' then 3011
                        when Segment.Segment = 'Innovation' then 3012
                        when Segment.Segment = 'Internal Communications' then 3013
                        when Segment.Segment = 'Internal Communictions' then 3013
                        when Segment.Segment = 'Internal PR' then 3013
                        when Segment.Segment = 'Investment' then 3014
                        when Segment.Segment = 'Investor Relations' then 3015
                        when Segment.Segment = 'IT Strategy' then 3016
                        when Segment.Segment = 'Market Access' then 3017
                        when Segment.Segment = 'Market Reseach/Insight' then 3018
                        when Segment.Segment = 'Market Research' then 3018
                        when Segment.Segment = 'Market Research / Insights' then 3018
                        when Segment.Segment = 'Market Research/Insight' then 3018
                        when Segment.Segment = 'Market Research/Insights' then 3018
                        when Segment.Segment = 'Marketing' then 3019
                        when Segment.Segment = 'Medical Affairs' then 3045
                        when Segment.Segment = 'Medical Education' then 3045
                        when Segment.Segment = 'Mobile Marketing' then 3022
                        when Segment.Segment = 'Offline' then 3023
                        when Segment.Segment = 'Planning and Buying Agency' then 3024
                        when Segment.Segment = 'Policy' then 3025
                        when Segment.Segment = 'Public Affairs' then 3027
                        when Segment.Segment = 'Pure Player' then 3028
                        when Segment.Segment = 'Recruitment Marketing' then 3029
                        when Segment.Segment = 'Sale' then 3030
                        when Segment.Segment = 'Sales' then 3030
                        when Segment.Segment = 'Shopping Marketing' then 3031
                        when Segment.Segment = 'Social Media' then 3032
                        when Segment.Segment = 'Sponsorship' then 3033
                        when Segment.Segment = 'strategy' then 3034
                        when Segment.Segment = 'Supply Side Platform' then 3035
                        when Segment.Segment = 'technology' then 3036
                        when Segment.Segment = 'Technology PR' then 3037
                        when Segment.Segment = 'TV' then 3038
                else '' --Segment.Segment
                end, ''), '3042') as 'FUNCTIONAL_EXPERTISE_ID' --> VC functional_expertise.name
-- select count(*) -- 144592 -- select distinct Segment.Segment --top 10 * --240342 26822
from Contacts CL
left join ( select si.ObjectId, s.segment from SgmtInstances si left join segments s on s.SegmentId = si.SegmentId where s.segment is not null) Segment on Segment.ObjectId = cl.contactid
--left join ( select distinct c.ContactId, s.segment from contacts c left join SgmtInstances si on c.contactid = si.ObjectId left join segments s on s.SegmentId = si.SegmentId where s.segment is not null ) Segment on CL.contactid = Segment.contactid
--left join ( select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid ) skill on CL.contactid = skill.contactid
where cl.descriptor = 2
)

--select FUNCTIONAL_EXPERTISE_ID,count(*) from fe group by FUNCTIONAL_EXPERTISE_ID
--and CL.FirstName = 'Katy'
--and CL.FirstName = 'Lisa' and CL.LastName = 'Seukeran'
--or CL.FirstName = 'Kate' and CL.LastName = 'Tagge'
--or CL.FirstName = 'Nicole' and CL.LastName = 'Yost'
--or CL.FirstName = 'Nicole' and CL.LastName = 'Martin'
--or CL.FirstName = 'Alexandra' and CL.LastName = 'Martin'
--and CL.ContactId in ('110998-3207-1554','110452-3164-11130','110393-3899-1662','110387-4971-1110','110362-5188-1540','110256-8229-9337','110245-1034-8294','110206-7622-13100','110129-5026-15322','110046-5356-15347','') ;
--and CL.ContactId in ('100593-8845-12136') --('110387-4971-1110') 

select * from fe --85804


-- SUB FUNCTIONAL EXPERTISE
with fe as (
select
                  distinct CL.ContactId
                --, CL.FirstName, CL.LastName
                , Coalesce(NULLIF(case
                        when Segment.Segment = 'Advertising' then 2984
                        when Segment.Segment = 'Analytics' then 2985
                        when Segment.Segment = 'B2B' then 2986
                        when Segment.Segment = 'Branding and Design' then 2987
                        when Segment.Segment = 'Branding/Design' then 2987
                        when Segment.Segment = 'Broadcasting PR' then 2988
                        when Segment.Segment = 'Business Intelligence' then 2989
                        when Segment.Segment = 'Comms' then 2990
                        when Segment.Segment = 'Consumer' then 2991
                        when Segment.Segment = 'Consumer PR' then 2991
                        when Segment.Segment = 'Content' then 2993
                        when Segment.Segment = 'Corporate' then 2994
                        when Segment.Segment = 'Corporate PR' then 2994
                        when Segment.Segment = 'Cross' then 2995
                        when Segment.Segment = 'CSR' then 2996
                        when Segment.Segment = 'Data Management Platform' then 2997
                        when Segment.Segment = 'Demand Side Platform' then 2998
                        when Segment.Segment = 'Digital' then 2999
                        when Segment.Segment = 'Digital Advertising' then 3000
                        when Segment.Segment = 'Digital Marketing' then 3001
                        when Segment.Segment = 'E-Commerce' then 3002
                        when Segment.Segment = 'Employee Engagement' then 3003
                        when Segment.Segment = 'Events' then 3004
                        when Segment.Segment = 'Film' then 3005
                        when Segment.Segment = 'Finance' then 3006
                        when Segment.Segment = 'financial' then 3007
                        when Segment.Segment = 'Financial PR' then 3007
                        when Segment.Segment = 'Healthcare Advertising' then 3008
                        when Segment.Segment = 'Healthcare PR' then 3009
                        when Segment.Segment = 'HR' then 3010
                        when Segment.Segment = 'In-House Communications' then 3011
                        when Segment.Segment = 'Innovation' then 3012
                        when Segment.Segment = 'Internal Communications' then 3013
                        when Segment.Segment = 'Internal Communictions' then 3013
                        when Segment.Segment = 'Internal PR' then 3013
                        when Segment.Segment = 'Investment' then 3014
                        when Segment.Segment = 'Investor Relations' then 3015
                        when Segment.Segment = 'IT Strategy' then 3016
                        when Segment.Segment = 'Market Access' then 3017
                        when Segment.Segment = 'Market Reseach/Insight' then 3018
                        when Segment.Segment = 'Market Research' then 3018
                        when Segment.Segment = 'Market Research / Insights' then 3018
                        when Segment.Segment = 'Market Research/Insight' then 3018
                        when Segment.Segment = 'Market Research/Insights' then 3018
                        when Segment.Segment = 'Marketing' then 3019
                        when Segment.Segment = 'Medical Affairs' then 3045
                        when Segment.Segment = 'Medical Education' then 3045
                        when Segment.Segment = 'Mobile Marketing' then 3022
                        when Segment.Segment = 'Offline' then 3023
                        when Segment.Segment = 'Planning and Buying Agency' then 3024
                        when Segment.Segment = 'Policy' then 3025
                        when Segment.Segment = 'Public Affairs' then 3027
                        when Segment.Segment = 'Pure Player' then 3028
                        when Segment.Segment = 'Recruitment Marketing' then 3029
                        when Segment.Segment = 'Sale' then 3030
                        when Segment.Segment = 'Sales' then 3030
                        when Segment.Segment = 'Shopping Marketing' then 3031
                        when Segment.Segment = 'Social Media' then 3032
                        when Segment.Segment = 'Sponsorship' then 3033
                        when Segment.Segment = 'strategy' then 3034
                        when Segment.Segment = 'Supply Side Platform' then 3035
                        when Segment.Segment = 'technology' then 3036
                        when Segment.Segment = 'Technology PR' then 3037
                        when Segment.Segment = 'TV' then 3038
                else '' --Segment.Segment
                end, ''), '3042') as 'FUNCTIONAL_EXPERTISE_ID' --> VC functional_expertise.name

		--, skill.skill as 'SUB-FUNCTIONAL_EXPERTISE(old)' --> VC sub_functional_expertise.name
		, case
                        when skill.skill is null then ''
                        when skill.skill = 'Ad hoc' then 'Ad hoc'
                        when skill.skill = 'Ad network' then 'Ad network'
                        when skill.skill = 'Adtech' then 'Ad Tech'
                        when skill.skill = 'Aerospace' then 'Aerospace'
                        when skill.skill = 'Affiliates' then 'Affiliates'
                        when skill.skill = 'Agriculture' then 'Agriculture'
                        when skill.skill = 'Analytics' then 'Analytics'
                        when skill.skill = 'Android' then 'Android'
                        when skill.skill = 'Animal Health' then 'Animal Health'
                        when skill.skill = 'Apps' then 'Apps'
                        when skill.skill = 'Arabic Speaker' then 'Arabic Speaker'
                        when skill.skill = 'Arabic Writer' then 'Arabic Writer'
                        when skill.skill = 'Architecture' then 'Architecture'
                        when skill.skill = 'Arts/Culture' then 'Arts and Culture'
                        when skill.skill = 'Asset Management' then 'Asset Management'
                        when skill.skill = 'Automotive' then 'Automotive'
                        when skill.skill = 'AV' then 'AV'
                        when skill.skill = 'B2B' then 'B2B'
                        when skill.skill = 'B2C' then 'B2C'
                        when skill.skill = 'Beauty' then 'Beauty'
                        when skill.skill = 'Blogger Outreach' then 'Blogger Outreach'
                        when skill.skill = 'Brand Development' then 'Brand Development'
                        when skill.skill = 'Brand Engagement' then 'Brand Engagement'
                        when skill.skill = 'Brand Innovation' then 'Brand Innovation'
                        when skill.skill = 'Brand Management' then 'Brand Management'
                        when skill.skill = 'Brand Strategy' then 'Brand Strategy'
                        when skill.skill = 'Business Development' then 'Business Development'
                        when skill.skill = 'Business Intelligence' then 'Business Intelligence'
                        when skill.skill = 'C#' then 'C#'
                        when skill.skill = 'CakePHP' then 'CakePHP'
                        when skill.skill = 'Cardiovascular Metabolic' then 'Cardiovascular Metabolic'
                        when skill.skill = 'Central Government' then 'Central Government'
                        when skill.skill = 'Charities' then 'Charities'
                        when skill.skill = 'Communications Planning' then 'Communications Planning'
                        when skill.skill = 'Conservative' then 'Conservative'
                        when skill.skill = 'Construction' then 'Construction'
                        when skill.skill = 'Consumer Health' then 'Consumer Health'
                        when skill.skill = 'Content Management' then 'Content Management'
                        when skill.skill = 'Copywriting' then 'Copywriting'
                        when skill.skill = 'Corporate Affairs' then 'Corporate Affairs'
                        when skill.skill = 'Creative' then 'Creative'
                        when skill.skill = 'Crisis & Issues Management' then 'Crisis & Issues Management'
                        when skill.skill = 'CRM' then 'CRM'
                        when skill.skill = 'CSR' then 'CSR'
                        when skill.skill = 'Data Analytics' then 'Data Analytics'
                        when skill.skill = 'Defence' then 'Defence'
                        when skill.skill = 'Diabetes' then 'Diabetes'
                        when skill.skill = 'Digital' then 'Digital'
                        when skill.skill = 'Display' then 'Display'
                        when skill.skill = 'Drink' then 'Drink'
                        when skill.skill = 'Drupal' then 'Drupal'
                        when skill.skill = 'Econometrics' then 'Econometrics'
                        when skill.skill = 'Editing' then 'Editing'
                        when skill.skill = 'Education' then 'Education'
                        when skill.skill = 'Email Marketing' then 'Email Marketing'
                        when skill.skill = 'EMEA Experience' then 'EMEA Experience'
                        when skill.skill = 'Employer Branding' then 'Employer Branding'
                        when skill.skill = 'Energy' then 'Energy'
                        when skill.skill = 'Engineering' then 'Engineering'
                        when skill.skill = 'English Speaker' then 'English Speaker'
                        when skill.skill = 'Entertainment' then 'Entertainment'
                        when skill.skill = 'Ethical' then 'Ethical'
                        when skill.skill = 'EU Political Experience' then 'EU Political Experience'
                        when skill.skill = 'Events' then 'Events'
                        when skill.skill = 'Experiential' then 'Experiential'
                        when skill.skill = 'Fashion' then 'Fashion'
                        when skill.skill = 'Fieldwork' then 'Fieldwork'
                        when skill.skill = 'Film' then 'Film'
                        when skill.skill = 'Financial Services' then 'Financial Services'
                        when skill.skill = 'Financial Tech' then 'Financial Tech'
                        when skill.skill = 'FMCG' then 'FMCG'
                        when skill.skill = 'Food' then 'Food'
                        when skill.skill = 'French Speaker' then 'French Speaker'
                        when skill.skill = 'Gambling' then 'Gambling'
                        when skill.skill = 'Gaming' then 'Gaming'
                        when skill.skill = 'German Speaker' then 'German Speaker'
                        when skill.skill = 'Global Experience' then 'Global Experience'
                        when skill.skill = 'Government' then 'Government'
                        when skill.skill = 'Health Economics' then 'Health Economics'
                        when skill.skill = 'Healthcare' then 'Healthcare'
                        when skill.skill = 'Hospitality' then 'Hospitality'
                        when skill.skill = 'HR' then 'HR'
                        when skill.skill = 'HTML' then 'HTML'
                        when skill.skill = 'Industrials' then 'Industrials'
                        when skill.skill = 'Infrastructure' then 'Infrastructure'
                        when skill.skill = 'Innovation' then 'Innovation'
                        when skill.skill = 'Insight' then 'Insight'
                        when skill.skill = 'Insurance' then 'Insurance'
                        when skill.skill = 'Interior Design' then 'Interior Design'
                        when skill.skill = 'Internal Communications' then 'Internal Communications'
                        when skill.skill = 'Investor Relations' then 'Investor Relations'
                        when skill.skill = 'IOS' then 'IOS'
                        when skill.skill = 'IT Services' then 'IT Services'
                        when skill.skill = 'Italian Speaker' then 'Italian Speaker'
                        when skill.skill = 'Javascript' then 'Javascript'
                        when skill.skill = 'Journalism' then 'Journalism'
                        when skill.skill = 'Labour' then 'Labour'
                        when skill.skill = 'Legal' then 'Legal'
                        when skill.skill = 'Lib Dem' then 'Lib Dem'
                        when skill.skill = 'Life Sciences' then 'Life Sciences'
                        when skill.skill = 'Lifestyle' then 'Lifestyle'
                        when skill.skill = 'Local Government' then 'Local Government'
                        when skill.skill = 'Logistics and Transport' then 'Logistics and Transport'
                        when skill.skill = 'Luxury' then 'Luxury'
                        when skill.skill = 'M&A' then 'M&A'
                        when skill.skill = 'Mandarin speaker' then 'Mandarin speaker'
                        when skill.skill = 'Market Access' then 'Market Access'
                        when skill.skill = 'MBA' then 'MBA'
                        when skill.skill = 'Media' then 'Media'
                        when skill.skill = 'Media Buying' then 'Media Buying'
                        when skill.skill = 'Media planning' then 'Media Planning'
                        when skill.skill = 'Media Relations' then 'Media Relations'
                        when skill.skill = 'Media Training' then 'Media Training'
                        when skill.skill = 'Medical Devices' then 'Medical Devices'
                        when skill.skill = 'medical education' then 'Medical Education'
                        when skill.skill = 'Medical Writing' then 'Medical Writing'
                        when skill.skill = 'Mobile' then 'Mobile'
                        when skill.skill = 'Music' then 'Music'
                        when skill.skill = 'Natural Resources' then 'Natural Resources'
                        when skill.skill = 'Neurology' then 'Neurology'
                        when skill.skill = 'NICE' then 'NICE'
                        when skill.skill = 'Nutrition' then 'Nutrition'
                        when skill.skill = 'Oncology' then 'Oncology'
                        when skill.skill = 'Oracle' then 'Oracle'
                        when skill.skill = 'Outcomes Research' then 'Outcomes Research'
                        when skill.skill = 'Packaging' then 'Packaging'
                        when skill.skill = 'Personal Finance' then 'Personal Finance'
                        when skill.skill = 'Pharamaceutical' then 'Pharmaceutical'
                        when skill.skill = 'Pharmaceutical' then 'Pharmaceutical'
                        when skill.skill = 'PHP' then 'PHP'
                        when skill.skill = 'Planning Permission' then 'Planning Permission'
                        when skill.skill = 'Policy' then 'Policy'
                        when skill.skill = 'PPC' then 'PPC'
                        when skill.skill = 'Private Equity' then 'Private Equity'
                        when skill.skill = 'Professional Services' then 'Professional Services'
                        when skill.skill = 'Programmatic (RTB)' then 'Programmatic (RTB)'
                        when skill.skill = 'Project Managment' then 'Project Management'
                        when skill.skill = 'Property' then 'Property'
                        when skill.skill = 'Public Sector PR' then 'Public Sector PR'
                        when skill.skill = 'Publicist' then 'Publicist'
                        when skill.skill = 'Publishing' then 'Publishing'
                        when skill.skill = 'Qualitative Research' then 'Qualitative Research'
                        when skill.skill = 'Quantative Research' then 'Quantative Research'
                        when skill.skill = 'Recruitment Advertising ' then 'Recruitment Advertising'
                        when skill.skill = 'Recruitment Marketing ' then 'Recruitment Marketing'
                        when skill.skill = 'Regulatory Affairs' then 'Regulatory Affairs'
                        when skill.skill = 'Renewable Energy' then 'Renewable Energy'
                        when skill.skill = 'Research' then 'Research'
                        when skill.skill = 'Respiratory Inflammation Autoimmune' then 'Respiratory Inflammation Autoimmune'
                        when skill.skill = 'Restaurant' then 'Restaurant'
                        when skill.skill = 'Retail' then 'Retail'
                        when skill.skill = 'Russian Speaker' then 'Russian Speaker'
                        when skill.skill = 'SaaS' then 'SaaS'
                        when skill.skill = 'Sales' then 'Sales'
                        when skill.skill = 'SEM' then 'SEM'
                        when skill.skill = 'SEO' then 'SEO'
                        when skill.skill = 'Social Media' then 'Social Media'
                        when skill.skill = 'Social Research' then 'Social Research'
                        when skill.skill = 'Spanish Speaker' then 'Spanish Speaker'
                        when skill.skill = 'Special Advisor' then 'Special Advisor'
                        when skill.skill = 'Sponsorship' then 'Sponsorship'
                        when skill.skill = 'Sport' then 'Sport'
                        when skill.skill = 'Sports PR' then 'Sports PR'
                        when skill.skill = 'Start-ups' then 'Start-ups'
                        when skill.skill = 'Supply Chain Management' then 'Supply Chain Management'
                        when skill.skill = 'Sustainability' then 'Sustainability'
                        when skill.skill = 'Symfony' then 'Symfony'
                        when skill.skill = 'Technology' then 'Technology'
                        when skill.skill = 'Telecoms' then 'Telecoms'
                        when skill.skill = 'Think Tank' then 'Think Tank'
                        when skill.skill = 'TMT' then 'TMT'
                        when skill.skill = 'Tobacco' then 'Tobacco'
                        when skill.skill = 'Trade Association' then 'Trade Association'
                        when skill.skill = 'Trade Marketing' then 'Trade Marketing'
                        when skill.skill = 'Travel and Tourism' then 'Travel and Tourism'
                        when skill.skill = 'TV' then 'TV'
                        when skill.skill = 'UI' then 'UI'
                        when skill.skill = 'Utilities' then 'Utilities'
                        when skill.skill = 'Web Content' then 'Web Content'
                        when skill.skill = 'Web Design' then 'Web Design'
                        when skill.skill = 'Wordpress' then 'Wordpress'
                        when skill.skill = 'Zend' then 'Zend'
                else ''
                end as 'SUB-FUNCTIONAL_EXPERTISE' --> VC sub_functional_expertise.name
-- select count(*) -- 144592 -- select distinct Segment.Segment --top 10 * --240342 26822
from Contacts CL
left join ( select si.ObjectId, s.segment from SgmtInstances si left join segments s on s.SegmentId = si.SegmentId where s.segment is not null) Segment on Segment.ObjectId = cl.contactid
--left join ( select distinct c.ContactId, s.segment from contacts c left join SgmtInstances si on c.contactid = si.ObjectId left join segments s on s.SegmentId = si.SegmentId where s.segment is not null ) Segment on CL.contactid = Segment.contactid
left join ( select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid ) skill on CL.contactid = skill.contactid
where cl.descriptor = 2
)
--select [SUB-FUNCTIONAL_EXPERTISE],count(*) from fe group by [SUB-FUNCTIONAL_EXPERTISE]
select  * from fe where [SUB-FUNCTIONAL_EXPERTISE] is not null and [SUB-FUNCTIONAL_EXPERTISE] <> ''