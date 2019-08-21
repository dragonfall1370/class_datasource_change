---PART 1: Import COMPANY

select concat('HS',CompanyID) as 'company-externalId'
, nullif(Name,'') as 'company-name'
, nullif(PhoneNumber,'') as 'company-phone'
, nullif(WebsiteURL,'') as 'company-website'
, stuff((coalesce(' ' + nullif(StreetAddress,''),'') + coalesce(', ' + nullif(StreetAddress2,''),'') 
	+ coalesce(', ' + nullif(City,''),'') + coalesce(', ' + nullif(State_Region,''),'') 
	+ coalesce(', ' + nullif(PostalCode,''),'') + coalesce(', ' + nullif(Country,''),'')),1,1,'') as 'company-locationName'
, stuff((coalesce(' ' + nullif(StreetAddress,''),'') + coalesce(', ' + nullif(StreetAddress2,''),'') 
	+ coalesce(', ' + nullif(City,''),'') + coalesce(', ' + nullif(State_Region,''),'')
	+ coalesce(', ' + nullif(PostalCode,''),'') + coalesce(', ' + nullif(Country,''),'')),1,1,'') as 'company-locationAddress'
, City as 'company-locationCity'
, State_Region as 'company-locationState'
, PostalCode as 'company-locationZipCode'
, Country
, case when Country = ' Australia' then 'AU'
	when Country = ' Israel' then 'IL'
	when Country = ' United Kingdom' then 'GB'
	when Country = 'AU' then 'AU'
	when Country = 'Australia' then 'AU'
	when Country = 'Belgium' then 'BE'
	when Country = 'Canada' then 'CA'
	when Country = 'China' then 'CN'
	when Country = 'Colombia' then 'CO'
	when Country = 'France' then 'FR'
	when Country = 'Germany' then 'DE'
	when Country = 'Hong Kong' then 'HK'
	when Country = 'India' then 'IN'
	when Country = 'Ireland' then 'IE'
	when Country = 'Israel' then 'IL'
	when Country = 'Japan' then 'JP'
	when Country = 'London' then 'GB'
	when Country = 'Netherlands' then 'NL'
	when Country = 'New Zealand' then 'NZ'
	when Country = 'NZ' then 'NZ'
	when Country = 'Pakistan' then 'PK'
	when Country = 'Papua New Guinea' then 'PG'
	when Country = 'Philippines' then 'PH'
	when Country = 'QLD, Australia.' then 'AU'
	when Country = 'Saudi Arabia' then 'SA'
	when Country = 'Singapore' then 'SG'
	when Country = 'South Australia' then 'AU'
	when Country = 'Spain' then 'ES'
	when Country = 'Switzerland' then 'CH'
	when Country = 'Turkey' then 'TR'
	when Country = 'UK' then 'GB'
	when Country = 'United Arab Emirates' then 'AE'
	when Country = 'United Kingdom' then 'GB'
	when Country = 'United States' then 'US'
	when Country = 'US' then 'US'
	when Country = 'USA' then 'US'
	when Country = 'Vietnam' then 'VN' 
	else NULL end as 'company-locationCountry'
, case when HubSpotOwner like '%Andrea%Samson%' then 'andrea.samson@emapta.com'
	when HubSpotOwner like '%Nick%Reyes%' then 'nick.reyes@emapta.com'
	when HubSpotOwner like '%Onboarding%Team%' then 'emapta.onboarding.team@emapta.com'
	when HubSpotOwner like '%Mary%Grace%Lucero%' then 'grace.lucero@emapta.com'
	when HubSpotOwner like '%Jem%Lopez%' then 'jem.lopez@emapta.com'
	when HubSpotOwner like '%Cris%Soliman%' then 'charisma.soliman@emapta.com'
	when HubSpotOwner like '%Yam%Quizon%' then 'yam.quizon@emapta.com'
	when HubSpotOwner like '%Jewel%Layug%' then 'jewel.layug@emapta.com'
	when HubSpotOwner like '%Rowena%Santos' then 'rowena.santos@emapta.com'
	when HubSpotOwner like '%Ben%van%de%Beld' then 'Ben.Vandebeld@emapta.com'
	when HubSpotOwner like '%Suzette%Casidsid' then 'suzette.casidsid@emapta.com'
	when HubSpotOwner like '%Jinky%Tallod' then 'jinky.tallod@emapta.com'
	when HubSpotOwner like '%Adrian%Banaag' then 'adrian.banaag@emapta.com'
	when HubSpotOwner like '%Karl%Mitmannsgruber%' then 'karl.mitmannsgruber@emapta.com'
	when HubSpotOwner like '%Ana%Suguitan%' then 'ana.suguitan@emapta.com'
	when HubSpotOwner like '%Angelo%De%Leon%' then 'angelo.deleon@emapta.com'
	when HubSpotOwner like '%Josh%Despuig%' then 'josh.despuig@emapta.com'
	when HubSpotOwner like '%Arthur%Herrera%' then 'arthur.herrera@emapta.com'
	when HubSpotOwner like '%Graciel%Litonjua%' then 'graciel.litonjua@emapta.com'
	when HubSpotOwner like '%Jasmin%Banayo%' then 'jasmin.banayo@emapta.com'
	when HubSpotOwner like '%Maria%Angela%Capispisan%' then 'angela.capispisan@emapta.com'
	when HubSpotOwner like '%Ruby%Ann%Del%Rosario%' then 'rubyann.delrosario@emapta.com'
	when HubSpotOwner like '%Lester%Cui%' then 'lester.cui@emapta.com'
	when HubSpotOwner like '%Katricia%de%Leon%' then 'katricia.deleon@emapta.com'
	when HubSpotOwner like '%Mary%Elizabeth%Ochea' then 'mary.ochea@emapta.com'
	when HubSpotOwner like '%Kris%Tinaza%' then 'kris.tinaza@emapta.com'
	when HubSpotOwner like '%Sharmae%Nepomuceno%' then 'sharm.nepomuceno@emapta.com'
	when HubSpotOwner like '%Shiela%Tantiado%' then 'shiela.tantiado@emapta.com'
	when HubSpotOwner like '%Ruby%Camposano' then 'ruby.camposano@emapta.com'
	else NULL end as 'contact-owners'
, concat(coalesce('Company Status: ' + nullif(CompanyStatus,'') + char(10),'')
	, coalesce('Create Date: ' + nullif(convert(varchar(10),CreateDate,120),'') + char(10),'')
	, coalesce('Last Modified Date: ' + nullif(convert(varchar(10),LastModifiedDate,120),'') + char(10),'')
	, coalesce('Lead Status: ' + nullif(LeadStatus,'') + char(10),'')
	, coalesce('Company Domain Name: ' + nullif(CompanyDomainName,'') + char(10),'')
	, coalesce('Recent Deal Close Date: ' + nullif(convert(varchar(10),RecentDealCloseDate,120),'') + char(10),'')
	, coalesce('Time of Last Session: ' + nullif(convert(varchar(10),TimeofLastSession,120),'') + char(10),'')
	, coalesce('Close Date: ' + nullif(convert(varchar(10),CloseDate,120),'') + char(10),'')
	, coalesce('Office Model: ' + nullif(OfficeModel,'') + char(10),'')
	, coalesce('Associated Deals: ' + nullif(convert(varchar(max),AssociatedDeals),'') + char(10),'')
	, coalesce('Recent Deal Amount: ' + nullif(convert(varchar(max),RecentDealAmount),'') + char(10),'')
	, coalesce('First Conversion Date: ' + nullif(convert(varchar(10),FirstConversionDate,120),'') + char(10),'')
	, coalesce('OriginalSourceType: ' + nullif(OriginalSourceType,'') + char(10),'')
	, coalesce('First Deal Created Date: ' + nullif(convert(varchar(10),FirstDealCreatedDate,120),'') + char(10),'')
	, coalesce('Other Source: ' + nullif(OtherSource,'') + char(10),'')
	, coalesce('Facebook Company Page: ' + nullif(FacebookCompanyPage,'') + char(10),'')
	, coalesce('LinkedIn Bio: ' + nullif(LinkedInBio,'') + char(10),'')
	, coalesce('First Conversion: ' + nullif(FirstConversion,'') + char(10),'')
	, coalesce('Number of child companies: ' + nullif(convert(varchar(max),Numberofchildcompanies),'') + char(10),'')
	, coalesce('HubSpot Owner: ' + nullif(HubSpotOwner,'') + char(10),'')
	, coalesce('About Us: ' + nullif(AboutUs,'') + char(10),'')
	, coalesce('Last Activity Date: ' + nullif(convert(varchar(10),LastActivityDate,120),'') + char(10),'')
	, coalesce('Next Activity Date: ' + nullif(convert(varchar(10),NextActivityDate,120),'') + char(10),'')
	, coalesce('LinkedIn Company Page: ' + nullif(LinkedInCompanyPage,'') + char(10),'')
	, coalesce('Recent Conversion Date: ' + nullif(convert(varchar(10),RecentConversionDate,120),'') + char(10),'')
	, coalesce('Lifecycle Stage: ' + nullif(LifecycleStage,'') + char(10),'')
	, coalesce('Last Contacted: ' + nullif(convert(varchar(10),LastContacted,120),'') + char(10),'')
	, coalesce('Recent Conversion: ' + nullif(RecentConversion,'') + char(10),'')
	, coalesce('HubSpot Team: ' + nullif(HubSpotTeam,'') + char(10),'')
	, coalesce('Twitter Bio: ' + nullif(TwitterBio,'') + char(10),'')
	, coalesce('Web Technologies: ' + nullif(WebTechnologies,'') + char(10),'')
	, coalesce('FTE: ' + nullif(FTE,'') + char(10),'')
	, coalesce('Office Location: ' + nullif(OfficeLocation,'') + char(10),'')
	, coalesce('First Contact Create Date: ' + nullif(convert(varchar(10),FirstContactCreateDate,120),'') + char(10),'')
	, coalesce('Time Zone: ' + nullif(TimeZone,'') + char(10),'')
	, coalesce('Assigned BDM: ' + nullif(AssignedBDM,'') + char(10),'')
	, coalesce('Year Founded: ' + nullif(convert(varchar(max),YearFounded),'') + char(10),'')
	, coalesce('Twitter Handle: ' + nullif(TwitterHandle,'') + char(10),'')
	, coalesce('Google Plus Page: ' + nullif(GooglePlusPage,'') + char(10),'')
	, coalesce('Description: ' + nullif(Description,'') + char(10),'')
	, coalesce('Annual Revenue: ' + nullif(convert(varchar(max),AnnualRevenue),'') + char(10),'')
	, coalesce('Industry: ' + nullif(Industry,'') + char(10),'')
	, coalesce('Is Public: ' + nullif(IsPublic,'') + char(10),'')
	, coalesce('Parent Company: ' + nullif(ParentCompany,''),'')
	) as 'company-note'
from Company
where CompanyID not in (select DBCompanyID from MatchedPRODCompany)
order by CompanyID

---PART 2: Update COMPANY externalID in PROD
select concat('HS',DBCompanyID) as DBCompanyID, CompanyName, PRODVCID 
from MatchedPRODCompany


