

with 
 --indsec as (SELECT distinct indsec.reference, string_agg(MN.DESCRIPTION,',') as DESCRIPTION FROM PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where MN.ID is not null and LANGUAGE = 10010 group by indsec.reference)
 indsec as (select REFERENCE,DESCRIPTION from PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where LANGUAGE = 10010)



select --top 10
	  cj.JOB as 'position-externalId', jg.JOB_ID as '#JOB_ID'
       --, indsec.indsec
       , case 
when description = '.Com' then '.Com'
when description = 'Accountancy' then 'Accountancy'
when description = 'Aggregator Company' then 'Aggregator Company'
when description = 'Agriculture/Farming' then 'Manufacturing & Production'
when description = 'Airlines' then 'Travel and Tourism'
when description = 'Architects/Surveyors' then 'Building and Construction'
when description = 'Armed Forces' then 'Government'
when description = 'Audio/Visual' then 'Media & Ad Tech'
when description = 'Banking' then 'Banking'
when description = 'Building Societies' then 'Banking'
when description = 'Building/Construction' then 'Building and Construction'
when description = 'Business Consultancy' then 'Consultancy'
when description = 'Business Travel' then 'Travel and Tourism'
when description = 'Care' then 'Healthcare'
when description = 'Central Government' then 'Government'
when description = 'Charities' then 'Not for Profit and Charities'
when description = 'Charity' then 'Not for Profit and Charities'
when description = 'Chemicals' then 'Engineering, Manufacturing and Production'
when description = 'Commercial Bank' then 'Banking'
when description = 'Computer Hardware' then 'IT'
when description = 'Computer Software' then 'IT '
when description = 'Consultancy' then 'Consultancy'
when description = 'Data Company' then 'Data Analytics'
when description = 'Development Agencies' then 'Goverenment'
when description = 'Digital Agency' then 'Digital Agency'
when description = 'Education' then 'Education and Training'
when description = 'Electrical' then 'Utilities'
when description = 'Electricity and Gas' then 'Utilities'
when description = 'Engineering / Construction / Manufacturing' then 'Engineering, Manufacturing and Production'
when description = 'Entertainment' then 'Entertainment'
when description = 'Fashion' then 'Fashion'
when description = 'Film/TV/Radio' then 'Entertainment'
when description = 'Finance and Banking' then 'Banking'
when description = 'Financial Services' then 'Financial Services'
when description = 'FinTech' then 'FinTech'
when description = 'Food/Beverage/Catering' then 'Retail'
when description = 'Freight/Haulage' then 'Logistics Distribution and Supply Chain'
when description = 'Gaming' then 'Gaming'
when description = 'General Insurance' then 'General Insurance'
when description = 'General Retailers' then 'Retail'
when description = 'Health' then 'HealthCare'
when description = 'Hedge Fund' then 'Financial Services'
when description = 'Hotel/Leisure/Restaurant' then 'Leisure and Sport'
when description = 'Housing Associations' then 'Social Care'
when description = 'Import/Export' then 'Logistics Distribution and Supply Chain'
when description = 'Insight Consultancy' then 'Insight Consultancy '
when description = 'Insurance' then 'Insurance'
when description = 'Investment Banking' then 'Financial Services'
when description = 'IT Consultancy' then 'IT Consultancy'
when description = 'IT/Telecomms' then 'Telecommunications'
when description = 'Legal' then 'Legal'
when description = 'Legal Services' then 'Legal'
when description = 'Libraries' then 'Leisure and Sport'
when description = 'Life & Pensions' then 'Life and Pensions'
when description = 'Lloyds Market' then 'Lloyds Market'
when description = 'Local Government' then 'Government'
when description = 'Manufacturing & Construction' then 'Engineering, Manufacturing and Production'
when description = 'Marketing Agency' then 'Marketing Agency'
when description = 'Media' then 'Media & Ad Tech'
when description = 'Mining' then 'Utilities'
when description = 'Mobile' then 'Telecommunications'
when description = 'Museums & Galleries' then 'Arts'
when description = 'Non-PASA Hospital/Healthcare' then 'Healthcare'
when description = 'Not For Profit' then 'Not for Profit and Charities'
when description = 'Other Public Transport' then 'Transport and Rail'
when description = 'PASA' then 'Financial Services'
when description = 'Petrol/Oil/Fuel' then 'Oil and Gas'
when description = 'Pharmaceuticals' then 'Pharmaceuticals'
when description = 'Pharmacuetical/Petrochemical' then 'Pharmaceuticals'
when description = 'PR/Advertising/Marketing' then 'Marketing'
when description = 'Prisons' then 'Government'
when description = 'Private Banking' then 'Private Banking'
when description = 'Private Schools and Colleges' then 'Education and Training'
when description = 'Public Sector' then 'Government'
when description = 'Public Services' then 'Government'
when description = 'Rail' then 'Transport and Rail'
when description = 'Recruitment' then 'Recruitment Consultancy'
when description = 'Reinsurance' then 'Insurance'
when description = 'Research' then 'Science and Research'
when description = 'Retail' then 'Retail'
when description = 'Retail Banking' then 'Banking'
when description = 'Security' then 'Security'
when description = 'Shipping' then 'Logistics Distribution and Supply Chain'
when description = 'Software House' then 'Software House'
when description = 'Sporting' then 'Leisure and Sport'
when description = 'Telecommunications & Cable' then 'Telecommunications'
when description = 'Telecoms' then 'Telecommunications'
when description = 'Textile' then 'Fashion'
when description = 'Tour Operators' then 'Travel and Tourism'
when description = 'Transport' then 'Transport and Rail'
when description = 'Transport Contractors' then 'Transport and Rail'
when description = 'Travel' then 'Travel and Tourism'
when description = 'Utilities' then 'Utilities'
when description = 'Vehicle Manufacturers' then 'Automotive'
when description = 'Water' then 'Utilities'
--else concat('__',description)
end as industry       
       
       
-- select *	  
from PROP_X_CLIENT_JOB cj  --8076 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
--left join (SELECT distinct indsec.reference, string_agg(MN.DESCRIPTION,', ') as indsec FROM PROP_IND_SECT indsec INNER JOIN MD_MULTI_NAMES MN ON MN.ID = indsec.industry where MN.ID is not null and LANGUAGE = 10010 group by indsec.reference) indsec on indsec.reference = cj.JOB --Job Sector
left join indsec ON indsec.reference = cj.job -- industry
where indsec.reference is not null --and indsec.DESCRIPTION <> ''
--and jg.JOB_ID = 882981


	  