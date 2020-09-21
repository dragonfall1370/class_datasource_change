/*
with
  skill as (select REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL where LANGUAGE = 10010 )
, location as (select REFERENCE,DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION where LANGUAGE = 10010)
, indsec as (select REFERENCE,DESCRIPTION from PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where LANGUAGE = 10010)
, cat as (select REFERENCE,DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY where LANGUAGE = 10010)


select --top 10
         pg.REFERENCE as 'candidate-externalId', pg.person_id
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'

       , location.DESCRIPTION 'candidate-location'
	, skill.DESCRIPTION 'candidate-skills'
       --, cat.DESCRIPTION as 'candidate-category'
       --, indsec.DESCRIPTION as 'candidate-industry'
       , case 
when indsec.description = '.Com' then '.Com'
when indsec.description = 'Accountancy' then 'Accountancy'
when indsec.description = 'Aggregator Company' then 'Aggregator Company'
when indsec.description = 'Agriculture/Farming' then 'Manufacturing & Production'
when indsec.description = 'Airlines' then 'Travel and Tourism'
when indsec.description = 'Architects/Surveyors' then 'Building and Construction'
when indsec.description = 'Armed Forces' then 'Government'
when indsec.description = 'Audio/Visual' then 'Media & Ad Tech'
when indsec.description = 'Banking' then 'Banking'
when indsec.description = 'Building Societies' then 'Banking'
when indsec.description = 'Building/Construction' then 'Building and Construction'
when indsec.description = 'Business Consultancy' then 'Consultancy'
when indsec.description = 'Business Travel' then 'Travel and Tourism'
when indsec.description = 'Care' then 'Healthcare'
when indsec.description = 'Central Government' then 'Government'
when indsec.description = 'Charities' then 'Not for Profit and Charities'
when indsec.description = 'Charity' then 'Not for Profit and Charities'
when indsec.description = 'Chemicals' then 'Engineering, Manufacturing and Production'
when indsec.description = 'Commercial Bank' then 'Banking'
when indsec.description = 'Computer Hardware' then 'IT'
when indsec.description = 'Computer Software' then 'IT '
when indsec.description = 'Consultancy' then 'Consultancy'
when indsec.description = 'Data Company' then 'Data Analytics'
when indsec.description = 'Development Agencies' then 'Goverenment'
when indsec.description = 'Digital Agency' then 'Digital Agency'
when indsec.description = 'Education' then 'Education and Training'
when indsec.description = 'Electrical' then 'Utilities'
when indsec.description = 'Electricity and Gas' then 'Utilities'
when indsec.description = 'Engineering / Construction / Manufacturing' then 'Engineering, Manufacturing and Production'
when indsec.description = 'Entertainment' then 'Entertainment'
when indsec.description = 'Fashion' then 'Fashion'
when indsec.description = 'Film/TV/Radio' then 'Entertainment'
when indsec.description = 'Finance and Banking' then 'Banking'
when indsec.description = 'Financial Services' then 'Financial Services'
when indsec.description = 'FinTech' then 'FinTech'
when indsec.description = 'Food/Beverage/Catering' then 'Retail'
when indsec.description = 'Freight/Haulage' then 'Logistics Distribution and Supply Chain'
when indsec.description = 'Gaming' then 'Gaming'
when indsec.description = 'General Insurance' then 'General Insurance'
when indsec.description = 'General Retailers' then 'Retail'
when indsec.description = 'Health' then 'HealthCare'
when indsec.description = 'Hedge Fund' then 'Financial Services'
when indsec.description = 'Hotel/Leisure/Restaurant' then 'Leisure and Sport'
when indsec.description = 'Housing Associations' then 'Social Care'
when indsec.description = 'Import/Export' then 'Logistics Distribution and Supply Chain'
when indsec.description = 'Insight Consultancy' then 'Insight Consultancy '
when indsec.description = 'Insurance' then 'Insurance'
when indsec.description = 'Investment Banking' then 'Financial Services'
when indsec.description = 'IT Consultancy' then 'IT Consultancy'
when indsec.description = 'IT/Telecomms' then 'Telecommunications'
when indsec.description = 'Legal' then 'Legal'
when indsec.description = 'Legal Services' then 'Legal'
when indsec.description = 'Libraries' then 'Leisure and Sport'
when indsec.description = 'Life & Pensions' then 'Life and Pensions'
when indsec.description = 'Lloyds Market' then 'Lloyds Market'
when indsec.description = 'Local Government' then 'Government'
when indsec.description = 'Manufacturing & Construction' then 'Engineering, Manufacturing and Production'
when indsec.description = 'Marketing Agency' then 'Marketing Agency'
when indsec.description = 'Media' then 'Media & Ad Tech'
when indsec.description = 'Mining' then 'Utilities'
when indsec.description = 'Mobile' then 'Telecommunications'
when indsec.description = 'Museums & Galleries' then 'Arts'
when indsec.description = 'Non-PASA Hospital/Healthcare' then 'Healthcare'
when indsec.description = 'Not For Profit' then 'Not for Profit and Charities'
when indsec.description = 'Other Public Transport' then 'Transport and Rail'
when indsec.description = 'PASA' then 'Financial Services'
when indsec.description = 'Petrol/Oil/Fuel' then 'Oil and Gas'
when indsec.description = 'Pharmaceuticals' then 'Pharmaceuticals'
when indsec.description = 'Pharmacuetical/Petrochemical' then 'Pharmaceuticals'
when indsec.description = 'PR/Advertising/Marketing' then 'Marketing'
when indsec.description = 'Prisons' then 'Government'
when indsec.description = 'Private Banking' then 'Private Banking'
when indsec.description = 'Private Schools and Colleges' then 'Education and Training'
when indsec.description = 'Public Sector' then 'Government'
when indsec.description = 'Public Services' then 'Government'
when indsec.description = 'Rail' then 'Transport and Rail'
when indsec.description = 'Recruitment' then 'Recruitment Consultancy'
when indsec.description = 'Reinsurance' then 'Insurance'
when indsec.description = 'Research' then 'Science and Research'
when indsec.description = 'Retail' then 'Retail'
when indsec.description = 'Retail Banking' then 'Banking'
when indsec.description = 'Security' then 'Security'
when indsec.description = 'Shipping' then 'Logistics Distribution and Supply Chain'
when indsec.description = 'Software House' then 'Software House'
when indsec.description = 'Sporting' then 'Leisure and Sport'
when indsec.description = 'Telecommunications & Cable' then 'Telecommunications'
when indsec.description = 'Telecoms' then 'Telecommunications'
when indsec.description = 'Textile' then 'Fashion'
when indsec.description = 'Tour Operators' then 'Travel and Tourism'
when indsec.description = 'Transport' then 'Transport and Rail'
when indsec.description = 'Transport Contractors' then 'Transport and Rail'
when indsec.description = 'Travel' then 'Travel and Tourism'
when indsec.description = 'Utilities' then 'Utilities'
when indsec.description = 'Vehicle Manufacturers' then 'Automotive'
when indsec.description = 'Water' then 'Utilities'
--else concat('__',indsec.description)
end as industry            

-- select count(*) -- select top 10 *
from PROP_PERSON_GEN pg
--left join location on location.reference = pg.reference
--left join skill on skill.reference = pg.reference
--left join cat on cat.reference = pg.reference
left join indsec on indsec.reference = pg.reference
where 
--pg.reference in (116674167980)
--pg.person_id in    (1094197,1098818,1103933,1120519, 1092301, 1097091, 1103039) 
indsec.reference is not null
*/


--select
--         distinct pg.REFERENCE as 'candidate-externalId', pg.person_id
--	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
--	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
--	, trim(skill.DESCRIPTION) 'candidate-skills', lower(trim(skill.DESCRIPTION)) as 'tmp'
-- select count(*) -- select top 10 * -- 
select distinct trim(skill.DESCRIPTION) as name, lower(trim(skill.DESCRIPTION)) as 'tmp', count(*)
from PROP_PERSON_GEN pg
left join (select REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL where LANGUAGE = 10010 ) skill on skill.reference = pg.reference
where (skill.reference is not null and convert(varchar(max),trim(skill.DESCRIPTION)) <> '')
--and pg.person_id in (1128694)
--and skill.DESCRIPTION like '%business objects%'
group by skill.DESCRIPTION

