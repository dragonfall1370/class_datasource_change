select * from contact limit 100

select count(*) from contact_comment --408
select * from contact_comment

select count(*) from position_candidate_feedback -- 0
select * from position_candidate_feedback

select * from contact limit 100

select * from position_candidate
select * from position_description

--SELECT id, filename = STUFF((SELECT DISTINCT ',' + 'com_' + replace(filename,',','') from Attachments WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') FROM Attachments a where a.filename is not null GROUP BY id --29130
--SELECT distinct replace(replace(ref,'\\rsserver',''),'c:\','') ,filename, 'com_' + replace(filename,',','') as filename from Attachments where filename is not null and filename <> '' and filename like '%._%'
--select top 100 * from Attachments
--SELECT ref ,filename, 'com_' + replace(filename,',','') as filename from Attachments where filename is not null and filename <> '' and filename like '%._%'
--select * from bulk_upload_detail

select top 100 * from SgmtInstances;
select top 100 * from segments;
select top 100 * from  Contacts;
select --top 100 
        c.ContactId, c.FirstName, c.LastName, c.DisplayName
        --, si.*
        , s.segment
 from contacts c left join SgmtInstances si on c.contactid = si.ObjectId left join segments s on s.SegmentId = si.SegmentId where s.segment is not null
and c.lastname = 'Stolliday'


-- CANDIDATE = 2
select top 2000
                  CL.ContactId
                  --, CL.regdate
                , CL.FirstName, CL.LastName
                /*
		, CL.Department as 'department' --> VC contact.department
		, skills.skill as 'skill' --> VC contact.skills
		
		, ltrim(Stuff( Coalesce(' ' + NULLIF(CL.Address1, ''), '') + Coalesce(', ' + NULLIF(CL.Address2, ''), '') + Coalesce(', ' + NULLIF(CL.Address3, ''), ''), 1, 1, '') ) as 'address' --> VC common_location
                , ltrim(Stuff( Coalesce(' ' + NULLIF(CL.City, ''), '') + Coalesce(', ' + NULLIF(CL.Country, ''), ''), 1, 1, '') ) as 'location_name' --> VC common_location
		, Coalesce(NULLIF(CL.City, ''), CL.SubLocation) as 'city' --> VC common_location
		, CL.County as 'district' --> VC common_location
		, CL.Country as 'country' --> VC common_location (UK and France only)
		--, CL.PostCode as 'post_code' --> VC common_location
		
		--, CL.Sector as 'INDUSTRY' --> VC vertical.name
                , case
                        when CL.Sector = 'Ad Networks' then 'Advertising Networks/Adtech Agency'
                        when CL.Sector = 'Advertising' then 'Advertising Agency'
                        when CL.Sector = 'Branding/Design' then 'Branding and Design Agency'
                        when CL.Sector = 'Business Intelligence' then 'Business Intelligence Agency'
                        when CL.Sector = 'Consulting Services' then ''
                        when CL.Sector = 'Digital' then 'Digital Agency'
                        when CL.Sector = 'Market Research/Insights' then 'Market Research/Insights Agency'
                        when CL.Sector = 'Marketing' then 'Marketing Agency'
                        when CL.Sector = 'Marketing Services' then 'Marketing Agency'
                        when CL.Sector = 'Media' then 'Media Agency'
                        when CL.Sector = 'Medical Education' then 'Medical Education Agency'
                        when CL.Sector = 'Mobile Marketing' then 'Mobile Marketing Agency'
                        when CL.Sector = 'Non Dept Gov Body' then 'Non Dept Gov Body'
                        when CL.Sector = 'Pharmacuetical/Bio Tech' then 'Pharmaceutical/Bio Tech'
                        when CL.Sector = 'Platforms' then 'Technology'
                        when CL.Sector = 'PR and Communications' then 'PR and Communications Agency'
                        when CL.Sector = 'Public Affairs' then 'Public Affairs Agency'
                        when CL.Sector = 'Shopping Marketing' then 'Shopper Marketing Agency'
                        when CL.Sector = 'Social Media' then 'Social Media Agency'
                        when CL.Sector = 'Strategy Consulting' then 'Strategy Consulting Agency'
                        when CL.Sector = '' then 'Content Agency'
                        when CL.Sector is null then 'Content Agency'
                        else CL.Sector
                        end as 'INDUSTRY' --> VC vertical.name
                
                , CL.Segment as 'FUNCTIONAL_EXPERTISE(old)' --> VC functional_expertise.name
                */
                , Coalesce(NULLIF(case
                        when Segment.Segment = '' then ''
                        when Segment.Segment = 'Asset management' then ''
                        when Segment.Segment = 'Branding/Design' then 'Branding and Design'
                        when Segment.Segment = 'Broadcast' then ''
                        when Segment.Segment = 'Broadcasting Agency' then ''
                        when Segment.Segment = 'Broadcasting PR' then 'Broadcast PR'
                        when Segment.Segment = 'Business Services' then ''
                        when Segment.Segment = 'Commercial Support' then ''
                        when Segment.Segment = 'Comms' then 'Communications Planning'
                        when Segment.Segment = 'Consumer' then 'Consumer PR'
                        when Segment.Segment = 'Consumer Tech' then ''
                        when Segment.Segment = 'corporate' then 'Corporate PR'
                        when Segment.Segment = 'Cross' then 'Media Cross'
                        when Segment.Segment = 'Defence' then ''
                        when Segment.Segment = 'Education' then ''
                        when Segment.Segment = 'Energy' then ''
                        when Segment.Segment = 'Entertainment' then ''
                        when Segment.Segment = 'financial' then 'Financial PR'
                        when Segment.Segment = 'Financial Services' then ''
                        when Segment.Segment = 'FMCG' then ''
                        when Segment.Segment = 'healthcare' then ''
                        when Segment.Segment = 'Healthcare / Pharma' then ''
                        when Segment.Segment = 'HR' then 'Human Resources'
                        when Segment.Segment = 'Internal Communictions' then 'Internal Communications'
                        when Segment.Segment = 'Internal PR' then 'Internal Communications'
                        when Segment.Segment = 'Luxury' then ''
                        when Segment.Segment = 'Management Consultancy' then ''
                        when Segment.Segment = 'Market Reseach/Insight' then 'Market Research and Insights '
                        when Segment.Segment = 'Market Research' then 'Market Research and Insights '
                        when Segment.Segment = 'Market Research / Insights' then 'Market Research and Insights '
                        when Segment.Segment = 'Market Research/Insight' then 'Market Research and Insights '
                        when Segment.Segment = 'Market Research/Insights' then ''
                        when Segment.Segment = 'Marketing / Insight' then ''
                        when Segment.Segment = 'Marketing Services' then ''
                        when Segment.Segment = 'Marketing/Insight' then ''
                        when Segment.Segment = 'Media' then ''
                        when Segment.Segment = 'Medical Education' then 'Medical Affairs'
                        when Segment.Segment = 'Not for profit' then ''
                        when Segment.Segment = 'Office' then ''
                        when Segment.Segment = 'Planning and Buying Agency' then 'Planning and Buying'
                        when Segment.Segment = 'PR and Communications' then ''
                        when Segment.Segment = 'Production Agency' then ''
                        when Segment.Segment = 'Professional Services' then ''
                        when Segment.Segment = 'Project Management' then ''
                        when Segment.Segment = 'Public Relations' then ''
                        when Segment.Segment = 'Public Sector' then ''
                        when Segment.Segment = 'Pure Player' then 'Pure Play Digital '
                        when Segment.Segment = 'Retail' then ''
                        when Segment.Segment = 'Sale' then 'Sales'
                        when Segment.Segment = 'Shopping Marketing' then 'Shopper Marketing'
                        when Segment.Segment = 'Strategy Consultancy' then ''
                        when Segment.Segment = 'Strategy Consulting' then ''
                        when Segment.Segment = 'Supply Side Platform' then 'Supply Side Platform '
                        when Segment.Segment = 'technology' then 'Technology  '
                        when Segment.Segment = 'Transport' then ''
                        when Segment.Segment = 'Utilities' then ''
                        when Segment.Segment = 'Web' then ''
                else Segment.Segment
                end, ''), 'Others') as 'FUNCTIONAL_EXPERTISE' --> VC functional_expertise.name
                
		--, skill.skill as 'SUB-FUNCTIONAL_EXPERTISE(old)' --> VC sub_functional_expertise.name
		, case
                        when skill.skill = 'Advertising' then ''
                        when skill.skill = 'Consumer Technology' then ''
                        when skill.skill = 'Consumer Technology' then ''
                        when skill.skill = 'Crisis & Issues Managment' then 'Crisis & Issues Management'
                        when skill.skill = 'Data & Analysis' then 'Data Analytics'
                        when skill.skill = 'Defence and Security' then 'Defence'
                        when skill.skill = 'Field Marketing' then ''
                        when skill.skill = 'Field Sales' then ''
                        when skill.skill = 'Hibernate' then ''
                        when skill.skill = 'Information Architecture' then ''
                        when skill.skill = 'Java' then 'Javascript'
                        when skill.skill = 'Logistics & Express' then ''
                        when skill.skill = 'Magento' then ''
                        when skill.skill = 'Measurement & Evaluation' then 'Analytics'
                        when skill.skill = 'Modeling' then ''
                        when skill.skill = 'NodeJS' then ''
                        when skill.skill = 'Objective-c' then ''
                        when skill.skill = 'On Rails' then ''
                        when skill.skill = 'OOH' then ''
                        when skill.skill = 'OS' then ''
                        when skill.skill = 'OTC' then ''
                        when skill.skill = 'pharmaceuticals' then 'Pharamaceutical'
                        when skill.skill = 'Prestashop' then ''
                        when skill.skill = 'Pricing and Reimbursement' then ''
                        when skill.skill = 'Print' then ''
                        when skill.skill = 'Product Design' then ''
                        when skill.skill = 'Quantitative research' then 'Quantative Research'
                        when skill.skill = 'Spring' then ''
                        when skill.skill = 'SVN' then ''
                        when skill.skill = 'Swift' then ''
                        when skill.skill = 'Trade Activation' then ''
                        when skill.skill = 'Transport' then ''
                        when skill.skill = 'Travel & Tourism' then 'Travel and Tourism'
                        when skill.skill = 'Unix' then ''
                        when skill.skill = 'UX' then ''
                        when skill.skill = 'W3C compliance,' then ''
                        when skill.skill = 'Windows' then ''
                        when skill.skill = 'Wireless' then ''
                else skill.skill
                end as 'SUB-FUNCTIONAL_EXPERTISE' --> VC sub_functional_expertise.name
-- select count(*) -- 144592 --distinct CL.Sector --top 10 *
from Contacts CL
left join ( select distinct c.ContactId, s.segment from contacts c left join SgmtInstances si on c.contactid = si.ObjectId left join segments s on s.SegmentId = si.SegmentId where s.segment is not null ) Segment on CL.contactid = Segment.contactid
left join ( select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid ) skill on CL.contactid = skill.contactid
where cl.descriptor = 2
--and CL.FirstName = 'Katy'
and CL.ContactId in ('110998-3207-1554','110452-3164-11130','110393-3899-1662','110387-4971-1110','110362-5188-1540','110256-8229-9337','110245-1034-8294','110206-7622-13100','110129-5026-15322','110046-5356-15347','') ;
--('229753-1631-8315') 
--SELECT contactid, Sector, r1 = ROW_NUMBER() OVER (PARTITION BY contactid ORDER BY contactid desc) from Contacts




-- CONTACT = 1
with skill as (select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid)
select top 200
                  CL.ContactId
                , CL.FirstName, CL.LastName
                
		, CL.Department as 'department' --> VC contact.department
		, skills.skill as 'skill' --> VC contact.skills
		
		, ltrim(Stuff( Coalesce(' ' + NULLIF(CL.Address1, ''), '') + Coalesce(', ' + NULLIF(CL.Address2, ''), '') + Coalesce(', ' + NULLIF(CL.Address3, ''), ''), 1, 1, '') ) as 'address' --> VC common_location
                , ltrim(Stuff( Coalesce(' ' + NULLIF(CL.City, ''), '') + Coalesce(', ' + NULLIF(CL.Country, ''), ''), 1, 1, '') ) as 'location_name' --> VC common_location
		, Coalesce(NULLIF(CL.City, ''), CL.SubLocation) as 'city' --> VC common_location
		, CL.County as 'district' --> VC common_location
		, CL.Country as 'country' --> VC common_location (UK and France only)
		--, CL.PostCode as 'post_code' --> VC common_location
		
		--, CL.Sector as 'INDUSTRY' --> VC vertical.name
                , case
                        when CL.Sector = 'Ad Networks' then 'Advertising Networks/Adtech Agency'
                        when CL.Sector = 'Advertising' then 'Advertising Agency'
                        when CL.Sector = 'Branding/Design' then 'Branding and Design Agency'
                        when CL.Sector = 'Business Intelligence' then 'Business Intelligence Agency'
                        when CL.Sector = 'Consulting Services' then ''
                        when CL.Sector = 'Digital' then 'Digital Agency'
                        when CL.Sector = 'Market Research/Insights' then 'Market Research/Insights Agency'
                        when CL.Sector = 'Marketing' then 'Marketing Agency'
                        when CL.Sector = 'Marketing Services' then 'Marketing Agency'
                        when CL.Sector = 'Media' then 'Media Agency'
                        when CL.Sector = 'Medical Education' then 'Medical Education Agency'
                        when CL.Sector = 'Mobile Marketing' then 'Mobile Marketing Agency'
                        when CL.Sector = 'Non Dept Gov Body' then 'Non Dept Gov Body'
                        when CL.Sector = 'Pharmacuetical/Bio Tech' then 'Pharmaceutical and BioTech'
                        when CL.Sector = 'Platforms' then 'Technology'
                        when CL.Sector = 'PR and Communications' then 'PR and Communications Agency'
                        when CL.Sector = 'Public Affairs' then 'Public Affairs Agency'
                        when CL.Sector = 'Shopping Marketing' then 'Shopper Marketing Agency'
                        when CL.Sector = 'Social Media' then 'Social Media Agency'
                        when CL.Sector = 'Strategy Consulting' then 'Strategy Consulting Agency'
                        when CL.Sector = '' then 'Content Agency'
                        when CL.Sector is null then 'Content Agency'
                        else CL.Sector
                        end as 'INDUSTRY' --> VC vertical.name
                
                --, CL.Segment as 'FUNCTIONAL_EXPERTISE(old)' --> VC functional_expertise.name
                , Coalesce(NULLIF(case
                        when CL.Segment = '' then ''
                        when CL.Segment = 'Asset management' then ''
                        when CL.Segment = 'Branding/Design' then 'Branding and Design'
                        when CL.Segment = 'Broadcast' then ''
                        when CL.Segment = 'Broadcasting Agency' then ''
                        when CL.Segment = 'Broadcasting PR' then 'Broadcast PR'
                        when CL.Segment = 'Business Services' then ''
                        when CL.Segment = 'Commercial Support' then ''
                        when CL.Segment = 'Comms' then 'Communications Planning'
                        when CL.Segment = 'Consumer' then 'Consumer PR'
                        when CL.Segment = 'Consumer Tech' then ''
                        when CL.Segment = 'corporate' then 'Corporate PR'
                        when CL.Segment = 'Cross' then 'Media Cross'
                        when CL.Segment = 'Defence' then ''
                        when CL.Segment = 'Education' then ''
                        when CL.Segment = 'Energy' then ''
                        when CL.Segment = 'Entertainment' then ''
                        when CL.Segment = 'financial' then 'Financial PR'
                        when CL.Segment = 'Financial Services' then ''
                        when CL.Segment = 'FMCG' then ''
                        when CL.Segment = 'healthcare' then ''
                        when CL.Segment = 'Healthcare / Pharma' then ''
                        when CL.Segment = 'HR' then 'Human Resources'
                        when CL.Segment = 'Internal Communictions' then 'Internal Communications'
                        when CL.Segment = 'Internal PR' then 'Internal Communications'
                        when CL.Segment = 'Luxury' then ''
                        when CL.Segment = 'Management Consultancy' then ''
                        when CL.Segment = 'Market Reseach/Insight' then 'Market Research and Insights '
                        when CL.Segment = 'Market Research' then 'Market Research and Insights '
                        when CL.Segment = 'Market Research / Insights' then 'Market Research and Insights '
                        when CL.Segment = 'Market Research/Insight' then 'Market Research and Insights '
                        when CL.Segment = 'Market Research/Insights' then ''
                        when CL.Segment = 'Marketing / Insight' then ''
                        when CL.Segment = 'Marketing Services' then ''
                        when CL.Segment = 'Marketing/Insight' then ''
                        when CL.Segment = 'Media' then ''
                        when CL.Segment = 'Medical Education' then 'Medical Affairs'
                        when CL.Segment = 'Not for profit' then ''
                        when CL.Segment = 'Office' then ''
                        when CL.Segment = 'Planning and Buying Agency' then 'Planning and Buying'
                        when CL.Segment = 'PR and Communications' then ''
                        when CL.Segment = 'Production Agency' then ''
                        when CL.Segment = 'Professional Services' then ''
                        when CL.Segment = 'Project Management' then ''
                        when CL.Segment = 'Public Relations' then ''
                        when CL.Segment = 'Public Sector' then ''
                        when CL.Segment = 'Pure Player' then 'Pure Play Digital '
                        when CL.Segment = 'Retail' then ''
                        when CL.Segment = 'Sale' then 'Sales'
                        when CL.Segment = 'Shopping Marketing' then 'Shopper Marketing'
                        when CL.Segment = 'Strategy Consultancy' then ''
                        when CL.Segment = 'Strategy Consulting' then ''
                        when CL.Segment = 'Supply Side Platform' then 'Supply Side Platform '
                        when CL.Segment = 'technology' then 'Technology  '
                        when CL.Segment = 'Transport' then ''
                        when CL.Segment = 'Utilities' then ''
                        when CL.Segment = 'Web' then ''
                else CL.Segment
                end, ''), 'Others') as 'FUNCTIONAL_EXPERTISE' --> VC functional_expertise.name
                
		--, skill.skill as 'SUB-FUNCTIONAL_EXPERTISE(old)' --> VC sub_functional_expertise.name
		, case
                        when skill.skill = 'Advertising' then ''
                        when skill.skill = 'Consumer Technology' then ''
                        when skill.skill = 'Consumer Technology' then ''
                        when skill.skill = 'Crisis & Issues Managment' then 'Crisis & Issues Management'
                        when skill.skill = 'Data & Analysis' then 'Data Analytics'
                        when skill.skill = 'Defence and Security' then 'Defence'
                        when skill.skill = 'Field Marketing' then ''
                        when skill.skill = 'Field Sales' then ''
                        when skill.skill = 'Hibernate' then ''
                        when skill.skill = 'Information Architecture' then ''
                        when skill.skill = 'Java' then 'Javascript'
                        when skill.skill = 'Logistics & Express' then ''
                        when skill.skill = 'Magento' then ''
                        when skill.skill = 'Measurement & Evaluation' then 'Analytics'
                        when skill.skill = 'Modeling' then ''
                        when skill.skill = 'NodeJS' then ''
                        when skill.skill = 'Objective-c' then ''
                        when skill.skill = 'On Rails' then ''
                        when skill.skill = 'OOH' then ''
                        when skill.skill = 'OS' then ''
                        when skill.skill = 'OTC' then ''
                        when skill.skill = 'pharmaceuticals' then 'Pharamaceutical'
                        when skill.skill = 'Prestashop' then ''
                        when skill.skill = 'Pricing and Reimbursement' then ''
                        when skill.skill = 'Print' then ''
                        when skill.skill = 'Product Design' then ''
                        when skill.skill = 'Quantitative research' then 'Quantative Research'
                        when skill.skill = 'Spring' then ''
                        when skill.skill = 'SVN' then ''
                        when skill.skill = 'Swift' then ''
                        when skill.skill = 'Trade Activation' then ''
                        when skill.skill = 'Transport' then ''
                        when skill.skill = 'Travel & Tourism' then 'Travel and Tourism'
                        when skill.skill = 'Unix' then ''
                        when skill.skill = 'UX' then ''
                        when skill.skill = 'W3C compliance,' then ''
                        when skill.skill = 'Windows' then ''
                        when skill.skill = 'Wireless' then ''
                else skill.skill
                end as 'SUB-FUNCTIONAL_EXPERTISE' --> VC sub_functional_expertise.name
from Contacts CL
left join skill on CL.contactid = skill.contactid
left join (SELECT contactid, skill = STUFF((SELECT skill + char(10) FROM skill b WHERE b.contactid = a.contactid FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') FROM skill a GROUP BY contactid) skills on CL.contactid = skills.contactid
where cl.descriptor = 1
and CL.Segment = 'Technology'
and CL.ContactId in ('105532-3747-6307')


select CL.contactid,skills.skill, skill.skill
from Contacts CL
where cl.descriptor = 1
and CL.ContactId in ('107834-9627-7214')


------------------
-- VC CANDIDATE --
select cl.*
        ,ca.id
        ,ca.first_name,ca.last_name
        ,ca.skills
        ,ca.industry
        ,ca.current_location_id
-- select count(*) --69.631
from candidate ca left join common_location cl on ca.current_location_id = cl.id --where ca.current_location_id is null
--select distinct current_location_id from candidate except select id from common_location

----------------
-- VC CONTACT --
select cl.*
        ,co.first_name,last_name
        ,co.current_location_id
        ,co.department
        ,co.skills
-- select count(*) --16.610
from contact co left join common_location cl on co.current_location_id = cl.id where co.current_location_id is null

--select count(*) from common_location --159.871
--select id,address,location_name,city,district,country,post_code from common_location
--select distinct current_location_id from contact except select id from common_location