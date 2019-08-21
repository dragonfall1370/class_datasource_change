---PART 1: Import CONTACT
--ORIGINAL CONTACT CSV
select concat('HS',AssociatedCompanyID) as 'contact-companyId'
, concat('HS',ContactID) as 'contact-externalId'
, coalesce(nullif(ltrim(FirstName),''),'First name') as 'contact-firstName'
, coalesce(nullif(ltrim(LastName),''),concat('Last name',ContactID)) as 'contact-lastName'
, nullif(Email,'') as 'contact-email'
, coalesce(nullif(JobTitle,''),Position) as 'contact-jobTitle'
, ltrim(stuff(coalesce(nullif(MobilePhoneNumber,''),'') 
	+ coalesce(', ' + nullif(PhoneNumber,''),''),1,1,'')) as 'contact-phone'
, HubSpotOwner
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
, concat(coalesce('Last email name: ' + nullif(Lastemailname,'') + char(10),'')
	, coalesce('Emails Opened: ' + nullif(convert(varchar(max),EmailsOpened),'') + char(10),'')
	, coalesce('Total Revenue: ' + nullif(convert(varchar(max),TotalRevenue),'') + char(10),'')
	, coalesce('Postal Code: ' + nullif(PostalCode,'') + char(10),'')
	, coalesce('Recent Deal Close Date: ' + nullif(convert(varchar(10),RecentDealCloseDate,120),'') + char(10),'')
	, coalesce('Became a Marketing Qualified Lead Date: ' + nullif(convert(varchar(10),BecameaMarketingQualifiedLeadDate,120),'') + char(10),'')
	, coalesce('Skype ID: ' + nullif(SkypeID,'') + char(10),'')
	, coalesce('Last Meeting Booked Campaign: ' + nullif(LastMeetingBookedCampaign,'') + char(10),'')
	, coalesce('Close Date: ' + nullif(convert(varchar(10),CloseDate,120),'') + char(10),'')
	, coalesce('Message: ' + nullif(Message,'') + char(10),'')
	, coalesce('Associated Deals: ' + nullif(convert(varchar(max),AssociatedDeals),'') + char(10),'')
	, coalesce('Opted out of all email: ' + nullif(Optedoutofallemail,'') + char(10),'')
	, coalesce('Recent Deal Amount: ' + nullif(convert(varchar(max),RecentDealAmount),'') + char(10),'')
	, coalesce('Default HubSpot Blog Subscription: ' + nullif(DefaultHubSpotBlogSubscription,'') + char(10),'')
	, coalesce('Number of times contacted: ' + nullif(convert(varchar(max),Numberoftimescontacted),'') + char(10),'')
	, coalesce('Number of Sales Activities: ' + nullif(convert(varchar(max),NumberofSalesActivities),'') + char(10),'')
	, coalesce('First Conversion Date: ' + nullif(convert(varchar(10),FirstConversionDate,120),'') + char(10),'')
	, coalesce('Recent Sales Email Clicked Date: ' + nullif(convert(varchar(10),RecentSalesEmailClickedDate,120),'') + char(10),'')
	, coalesce('Recent Sales Email Opened Date: ' + nullif(convert(varchar(10),RecentSalesEmailOpenedDate,120),'') + char(10),'')
	, coalesce('Original Source: ' + nullif(OriginalSource,'') + char(10),'')
	, coalesce('First Deal Created Date: ' + nullif(convert(varchar(10),FirstDealCreatedDate,120),'') + char(10),'')
	, coalesce('Currently in workflow: ' + nullif(Currentlyinworkflow,'') + char(10),'')
	, coalesce('Last Meeting Booked Medium: ' + nullif(LastMeetingBookedMedium,'') + char(10),'')
	, coalesce('Create Date: ' + nullif(convert(varchar(10),CreateDate,120),'') + char(10),'')
	, coalesce('LinkedIn Bio: ' + nullif(LinkedInBio,'') + char(10),'')
	, coalesce('First Conversion: ' + nullif(FirstConversion,'') + char(10),'')
	, coalesce('Became a Sales Qualified Lead Date: ' + nullif(convert(varchar(10),BecameaSalesQualifiedLeadDate,120),'') + char(10),'')
	, coalesce('Last Meeting Booked: ' + nullif(convert(varchar(10),LastMeetingBooked,120),'') + char(10),'')
	, coalesce('City: ' + nullif(City,'') + char(10),'')
	, coalesce('Number of event completions: ' + nullif(convert(varchar(max),Numberofeventcompletions),'') + char(10),'')
	, coalesce('Became a Subscriber Date: ' + nullif(convert(varchar(10),BecameaSubscriberDate,120),'') + char(10),'')
	, coalesce('Email Confirmation Status: ' + nullif(EmailConfirmationStatus,'') + char(10),'')
	, coalesce('Event Revenue: ' + nullif(convert(varchar(max),EventRevenue),'') + char(10),'')
	, coalesce('Last Activity Date: ' + nullif(convert(varchar(10),LastActivityDate,120),'') + char(10),'')
	, coalesce('Next Activity Date: ' + nullif(convert(varchar(10),NextActivityDate,120),'') + char(10),'')
	, coalesce('Last Meeting Booked Source: ' + nullif(LastMeetingBookedSource,'') + char(10),'')
	, coalesce('State/Region: ' + nullif(State_Region,'') + char(10),'')
	, coalesce('Became an Opportunity Date: ' + nullif(convert(varchar(10),BecameanOpportunityDate,120),'') + char(10),'')
	, coalesce('Last email open date: ' + nullif(convert(varchar(10),Lastemailopendate,120),'') + char(10),'')
	, coalesce('Opted out of all email: ' + nullif(Optedoutofallemail,'') + char(10),'')
	, coalesce('Original Source Drill-Down 1: ' + nullif(OriginalSourceDrill_Down1,'') + char(10),'')
	, coalesce('Last email send date: ' + nullif(convert(varchar(10),Lastemailsenddate,120),'') + char(10),'')
	, coalesce('Recent Conversion Date: ' + nullif(convert(varchar(10),RecentConversionDate,120),'') + char(10),'')
	, coalesce('Became an Other Lifecycle Date: ' + nullif(convert(varchar(10),BecameanOtherLifecycleDate,120),'') + char(10),'')
	, coalesce('Original Source Drill-Down 2: ' + nullif(OriginalSourceDrill_Down2,'') + char(10),'')
	, coalesce('Lifecycle Stage: ' + nullif(LifecycleStage,'') + char(10),'')
	, coalesce('Last Contacted: ' + nullif(convert(varchar(10),LastContacted,120),'') + char(10),'')
	, coalesce('Street Address: ' + nullif(StreetAddress,'') + char(10),'')
	, coalesce('Recent Conversion: ' + nullif(RecentConversion,'') + char(10),'')
	, coalesce('Country: ' + nullif(Country,'') + char(10),'')
	, coalesce('LinkedIn Connections: ' + nullif(convert(varchar(max),LinkedInConnections),'') + char(10),'')
	, coalesce('Persona: ' + nullif(Persona,'') + char(10),'')
	, coalesce('Salutation: ' + nullif(Salutation,'') + char(10),'')
	, coalesce('Sends Since Last Engagement: ' + nullif(convert(varchar(max),SendsSinceLastEngagement),'') + char(10),'')
	, coalesce('HubSpot Owner: ' + nullif(HubSpotOwner,'') + char(10),'')
	, coalesce('Became a Customer Date: ' + nullif(convert(varchar(10),BecameaCustomerDate,120),'') + char(10),'')
	, coalesce('Became a Lead Date: ' + nullif(convert(varchar(10),BecameaLeadDate,120),'') + char(10),'')
	, coalesce('Recent Sales Email Replied Date: ' + nullif(convert(varchar(10),RecentSalesEmailRepliedDate,120),'') + char(10),'')
	, coalesce('Website URL: ' + nullif(WebsiteURL,'') + char(10),'')
	, coalesce('Hub Spot Score: ' + nullif(convert(varchar(max),HubSpotScore),'') + char(10),'')
	, coalesce('Twitter Username: ' + nullif(TwitterUsername,'') + char(10),'')
	, coalesce('First Referring Site: ' + nullif(FirstReferringSite,'') + char(10),'')
	, coalesce('LastReferring Site: ' + nullif(LastReferringSite,'') + char(10),'')
	, coalesce('Twitter Profile Photo: ' + nullif(TwitterProfilePhoto,'') + char(10),'')
	, coalesce('Days To Close: ' + nullif(convert(varchar(max),DaysToClose),'') + char(10),'')
	, coalesce('Annual Revenue: ' + nullif(convert(varchar(max),AnnualRevenue),'') + char(10),'')
	, coalesce('Fax Number: ' + nullif(FaxNumber,'') + char(10),'')
	, coalesce('Industry: ' + nullif(Industry,'') + char(10),'')
	, coalesce('First email send date: ' + nullif(convert(varchar(10),Firstemailsenddate,120),'') + char(10),'')
	, coalesce('Email Address Quarantined: ' + nullif(EmailAddressQuarantined,'') + char(10),'')
	, coalesce('Number of Employees: ' + nullif(convert(varchar(max),NumberofEmployees),'') + char(10),'')
	, coalesce('Emails Bounced: ' + nullif(convert(varchar(max),EmailsBounced),'') + char(10),'')
	, coalesce('Associated Company: ' + nullif(AssociatedCompany,''),'')
	) as 'contact-note'
from Contact
where ContactID not in (select AssignContactID from AssignPRODContact)

UNION ALL

--NEW DEFAULT CONTACTS FOR COMPANIES WITHOUT CONTACT
select concat('HS',ndc.AssociatedCompanyID) as 'contact-companyId'
, concat('HS',ndc.newDefaultContactID) as 'contact-externalId'
, ndc.NewDefaultFirstname as 'contact-firstName'
, ndc.NewDefaultLastname as 'contact-lastName'
, NULL
, NULL
, NULL
, ds.HubSpotOwner
, case when ds.HubSpotOwner like '%Andrea%Samson%' then 'andrea.samson@emapta.com'
	when ds.HubSpotOwner like '%Nick%Reyes%' then 'nick.reyes@emapta.com'
	when ds.HubSpotOwner like '%Onboarding%Team%' then 'emapta.onboarding.team@emapta.com'
	when ds.HubSpotOwner like '%Mary%Grace%Lucero%' then 'grace.lucero@emapta.com'
	when ds.HubSpotOwner like '%Jem%Lopez%' then 'jem.lopez@emapta.com'
	when ds.HubSpotOwner like '%Cris%Soliman%' then 'charisma.soliman@emapta.com'
	when ds.HubSpotOwner like '%Yam%Quizon%' then 'yam.quizon@emapta.com'
	when ds.HubSpotOwner like '%Jewel%Layug%' then 'jewel.layug@emapta.com'
	when ds.HubSpotOwner like '%Rowena%Santos' then 'rowena.santos@emapta.com'
	when ds.HubSpotOwner like '%Ben%van%de%Beld' then 'Ben.Vandebeld@emapta.com'
	when ds.HubSpotOwner like '%Suzette%Casidsid' then 'suzette.casidsid@emapta.com'
	when ds.HubSpotOwner like '%Jinky%Tallod' then 'jinky.tallod@emapta.com'
	when ds.HubSpotOwner like '%Adrian%Banaag' then 'adrian.banaag@emapta.com'
	when ds.HubSpotOwner like '%Karl%Mitmannsgruber%' then 'karl.mitmannsgruber@emapta.com'
	when ds.HubSpotOwner like '%Ana%Suguitan%' then 'ana.suguitan@emapta.com'
	when ds.HubSpotOwner like '%Angelo%De%Leon%' then 'angelo.deleon@emapta.com'
	when ds.HubSpotOwner like '%Josh%Despuig%' then 'josh.despuig@emapta.com'
	when ds.HubSpotOwner like '%Arthur%Herrera%' then 'arthur.herrera@emapta.com'
	when ds.HubSpotOwner like '%Graciel%Litonjua%' then 'graciel.litonjua@emapta.com'
	when ds.HubSpotOwner like '%Jasmin%Banayo%' then 'jasmin.banayo@emapta.com'
	when ds.HubSpotOwner like '%Maria%Angela%Capispisan%' then 'angela.capispisan@emapta.com'
	when ds.HubSpotOwner like '%Ruby%Ann%Del%Rosario%' then 'rubyann.delrosario@emapta.com'
	when ds.HubSpotOwner like '%Lester%Cui%' then 'lester.cui@emapta.com'
	when ds.HubSpotOwner like '%Katricia%de%Leon%' then 'katricia.deleon@emapta.com'
	when ds.HubSpotOwner like '%Mary%Elizabeth%Ochea' then 'mary.ochea@emapta.com'
	when ds.HubSpotOwner like '%Kris%Tinaza%' then 'kris.tinaza@emapta.com'
	when ds.HubSpotOwner like '%Sharmae%Nepomuceno%' then 'sharm.nepomuceno@emapta.com'
	when ds.HubSpotOwner like '%Shiela%Tantiado%' then 'shiela.tantiado@emapta.com'
	when ds.HubSpotOwner like '%Ruby%Camposano' then 'ruby.camposano@emapta.com'
	else NULL end as 'contact-owners'
, concat(coalesce('Deal ID: ' + nullif(convert(varchar(max),ndc.DealID),'') + char(10),'')
	, coalesce('Deal Stage: ' + nullif(ndc.DealStage,'') + char(10),'')
	, coalesce('Associated Company: ' + nullif(ds.AssociatedCompany,'') + char(10),'')
	) as 'contact-note'
from NewDefaultContact ndc
left join DealSales ds on ds.DealID = ndc.DealID

UNION ALL

--NEW DEFAULT CONTACTS WITH EMAIL FOR COMPANIES WITHOUT CONTACT
select concat('HS',AssociatedCompanyID) as 'contact-companyId'
, concat('HS',NewContactID) as 'contact-externalId'
, DefaultFirstNameWithMail as 'contact-firstName'
, DefaultLastNameWithMail as 'contact-lastName'
, nullif(NewDefaultContactWMail,'') as 'contact-email'
, NULL
, NULL
, HubSpotOwner
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
, concat('This is new contact added from Deal Sales',char(10)
	, coalesce('Deal ID: ' + nullif(convert(varchar(max),DealID),'') + char(10),'')
	, coalesce('Deal Stage: ' + nullif(DealStage,'') + char(10),'')
	, coalesce('Last Modified Date: ' + nullif(convert(varchar(10),LastModifiedDate,120),'') + char(10),'')
	, coalesce('Pipeline: ' + nullif(Pipeline,'') + char(10),'')
	, coalesce('Close Date: ' + nullif(convert(varchar(10),CloseDate,120),'') + char(10),'')
	, coalesce('Deal Type: ' + nullif(DealType,'') + char(10),'')
	, coalesce('OfficeModel: ' + nullif(OfficeModel,'') + char(10),'')
	, coalesce('Original Source Type: ' + nullif(OriginalSourceType,'') + char(10),'')
	, coalesce('CreateDate: ' + nullif(convert(varchar(10),CreateDate,120),'') + char(10),'')
	, coalesce('Closed Lost Reason: ' + nullif(ClosedLostReason,'') + char(10),'')
	, coalesce('HubSpot Owner: ' + nullif(HubSpotOwner,'') + char(10),'')
	, coalesce('Last Activity Date: ' + nullif(convert(varchar(10),LastActivityDate,120),'') + char(10),'')
	, coalesce('Owner Assigned Date: ' + nullif(convert(varchar(10),OwnerAssignedDate,120),'') + char(10),'')
	, coalesce('Number of Contacts: ' + nullif(convert(varchar(max),NumberofContacts),'') + char(10),'')
	, coalesce('Original Source Data 1: ' + nullif(OriginalSourceData1,'') + char(10),'')
	, coalesce('Original Source Data 2: ' + nullif(OriginalSourceData2,'') + char(10),'')
	, coalesce('HubSpot Team: ' + nullif(HubSpotTeam,'') + char(10),'')
	, coalesce('Deal Name: ' + nullif(DealName,'') + char(10),'')
	, coalesce('FTE: ' + nullif(convert(varchar(max),FTE),'') + char(10),'')
	, coalesce('Amount: ' + nullif(convert(varchar(max),Amount),'') + char(10),'')
	, coalesce('Office Location: ' + nullif(OfficeLocation,'') + char(10),'')
	, coalesce('Assigned BDM: ' + nullif(AssignedBDM,'') + char(10),'')
	, coalesce('Deal Description: ' + nullif(DealDescription,'') + char(10),'')
	, coalesce('Associated Company: ' + nullif(AssociatedCompany,''),'')
	) as 'contact-note'
from NewContactAdded

--PART 2: Cleanse skipped contacts
select c.id, c.first_name, c.last_name, c.email, c.company_id, cc.external_id, cc.name from contact c
left join company cc on cc.id = c.company_id
where c.deleted_timestamp is NULL
and c.email in (
'marka@nzinvest.co.nz',
'lester.cui@emapta.com',
'charisma.soliman@emapta.com',
'adrian.banaag@emapta.com',
'joan.tan@ethixbase.com',
'gregdt@installandfixsolutions.com.au',
'c.sadler@villarilawyers.com.au',
'tony.martin@sapphiresystems.com')

--PART 3: Update external ID for existing contacts
select MatchedVCId, Fullname, concat('HS',AssignContactID) as AssignContactID
from AssignPRODContact

--> ver2 (PROD)
with MinContact as (select min(AssignContactID) as AssignContactID, MatchedVCId from AssignPRODContact group by MatchedVCId having count(MatchedVCId) > 1)

select MatchedVCId, Fullname, concat('HS',AssignContactID) as DBAssignContactID
from AssignPRODContact
where AssignContactID not in (select AssignContactID from MinContact)

--PART 4: Update Contact Stage
---4.1. Script from TableInput
with NonContactCompany as (select DealID, DealStage, AssociatedCompanyID, AssociatedContactID
	from DealSales 
	where AssociatedCompanyID is not NULL
	and AssociatedContactID is NULL
	and DealID not in (select DealID from NewContactAdded)
	and DealID not in (select DealID from NewDefaultContact)) --Company without contact -> to be updated 2mr

, MaxContactID as (select AssociatedCompanyID, max(ContactID) as MaxContactID
	from Contact
	where AssociatedCompanyID in (select AssociatedCompanyID from NonContactCompany)
	group by AssociatedCompanyID)

, TotalDealSales as (

	select DealID, DealStage, AssociatedCompanyID, concat('HS',AssociatedContactID) as AssociatedContactID
	from DealSales
	where DealID not in (select DealID from NewContactAdded)
	and DealID not in (select DealID from NewDefaultContact)
	and AssociatedContactID is not NULL

UNION ALL

	select DealID, DealStage, ncc.AssociatedCompanyID, concat('HS',mc.MaxContactID)
	from NonContactCompany ncc
	left join MaxContactID mc on mc.AssociatedCompanyID = ncc.AssociatedCompanyID
	where MaxContactID is not NULL

UNION ALL
	select DealID, DealStage, AssociatedCompanyID, concat('HS',NewContactID) from NewContactAdded

UNION ALL
	select DealID, DealStage, AssociatedCompanyID, concat('HS',newDefaultContactID) from NewDefaultContact)

select DealStage
, DealID
, AssociatedContactID as ExternalContactID
, case when DealStage = 'Introduction' or DealStage = 'Qualification ' then 1
	when DealStage = 'Solution Presentation' or DealStage = 'Proposal' then 2
	when DealStage = 'Negotiation' then 3
	when DealStage = 'Closed Lost' or DealStage = 'Nurture' then 3
	when DealStage = 'Closed Won' then 4
	else NULL end as 'VCboard'
, case when DealStage = 'Closed Lost' or DealStage = 'Nurture' then 2
	else 1 end as 'VCstatus'
from TotalDealSales
---4.2. Update VC contact status
--external_id | deleted_timestamp
--board | status

--PART 4: Update contact Info as ContactComments
---4.1. Script from Input
select concat('HS',apc.AssignContactID) as AssignContactID
, -10 as user_id
, getdate() as comment_timestamp
, concat(coalesce('Last email name: ' + nullif(Lastemailname,'') + char(10),'')
	, coalesce('Emails Opened: ' + nullif(convert(varchar(max),EmailsOpened),'') + char(10),'')
	, coalesce('Total Revenue: ' + nullif(convert(varchar(max),TotalRevenue),'') + char(10),'')
	, coalesce('Postal Code: ' + nullif(PostalCode,'') + char(10),'')
	, coalesce('Recent Deal Close Date: ' + nullif(convert(varchar(10),RecentDealCloseDate,120),'') + char(10),'')
	, coalesce('Became a Marketing Qualified Lead Date: ' + nullif(convert(varchar(10),BecameaMarketingQualifiedLeadDate,120),'') + char(10),'')
	, coalesce('Skype ID: ' + nullif(SkypeID,'') + char(10),'')
	, coalesce('Last Meeting Booked Campaign: ' + nullif(LastMeetingBookedCampaign,'') + char(10),'')
	, coalesce('Close Date: ' + nullif(convert(varchar(10),CloseDate,120),'') + char(10),'')
	, coalesce('Message: ' + nullif(Message,'') + char(10),'')
	, coalesce('Associated Deals: ' + nullif(convert(varchar(max),AssociatedDeals),'') + char(10),'')
	, coalesce('Opted out of all email: ' + nullif(Optedoutofallemail,'') + char(10),'')
	, coalesce('Recent Deal Amount: ' + nullif(convert(varchar(max),RecentDealAmount),'') + char(10),'')
	, coalesce('Default HubSpot Blog Subscription: ' + nullif(DefaultHubSpotBlogSubscription,'') + char(10),'')
	, coalesce('Number of times contacted: ' + nullif(convert(varchar(max),Numberoftimescontacted),'') + char(10),'')
	, coalesce('Number of Sales Activities: ' + nullif(convert(varchar(max),NumberofSalesActivities),'') + char(10),'')
	, coalesce('First Conversion Date: ' + nullif(convert(varchar(10),FirstConversionDate,120),'') + char(10),'')
	, coalesce('Recent Sales Email Clicked Date: ' + nullif(convert(varchar(10),RecentSalesEmailClickedDate,120),'') + char(10),'')
	, coalesce('Recent Sales Email Opened Date: ' + nullif(convert(varchar(10),RecentSalesEmailOpenedDate,120),'') + char(10),'')
	, coalesce('Original Source: ' + nullif(OriginalSource,'') + char(10),'')
	, coalesce('First Deal Created Date: ' + nullif(convert(varchar(10),FirstDealCreatedDate,120),'') + char(10),'')
	, coalesce('Currently in workflow: ' + nullif(Currentlyinworkflow,'') + char(10),'')
	, coalesce('Last Meeting Booked Medium: ' + nullif(LastMeetingBookedMedium,'') + char(10),'')
	, coalesce('Create Date: ' + nullif(convert(varchar(10),CreateDate,120),'') + char(10),'')
	, coalesce('LinkedIn Bio: ' + nullif(LinkedInBio,'') + char(10),'')
	, coalesce('First Conversion: ' + nullif(FirstConversion,'') + char(10),'')
	, coalesce('Became a Sales Qualified Lead Date: ' + nullif(convert(varchar(10),BecameaSalesQualifiedLeadDate,120),'') + char(10),'')
	, coalesce('Last Meeting Booked: ' + nullif(convert(varchar(10),LastMeetingBooked,120),'') + char(10),'')
	, coalesce('City: ' + nullif(City,'') + char(10),'')
	, coalesce('Number of event completions: ' + nullif(convert(varchar(max),Numberofeventcompletions),'') + char(10),'')
	, coalesce('Became a Subscriber Date: ' + nullif(convert(varchar(10),BecameaSubscriberDate,120),'') + char(10),'')
	, coalesce('Email Confirmation Status: ' + nullif(EmailConfirmationStatus,'') + char(10),'')
	, coalesce('Event Revenue: ' + nullif(convert(varchar(max),EventRevenue),'') + char(10),'')
	, coalesce('Last Activity Date: ' + nullif(convert(varchar(10),LastActivityDate,120),'') + char(10),'')
	, coalesce('Next Activity Date: ' + nullif(convert(varchar(10),NextActivityDate,120),'') + char(10),'')
	, coalesce('Last Meeting Booked Source: ' + nullif(LastMeetingBookedSource,'') + char(10),'')
	, coalesce('State/Region: ' + nullif(State_Region,'') + char(10),'')
	, coalesce('Became an Opportunity Date: ' + nullif(convert(varchar(10),BecameanOpportunityDate,120),'') + char(10),'')
	, coalesce('Last email open date: ' + nullif(convert(varchar(10),Lastemailopendate,120),'') + char(10),'')
	, coalesce('Opted out of all email: ' + nullif(Optedoutofallemail,'') + char(10),'')
	, coalesce('Original Source Drill-Down 1: ' + nullif(OriginalSourceDrill_Down1,'') + char(10),'')
	, coalesce('Last email send date: ' + nullif(convert(varchar(10),Lastemailsenddate,120),'') + char(10),'')
	, coalesce('Recent Conversion Date: ' + nullif(convert(varchar(10),RecentConversionDate,120),'') + char(10),'')
	, coalesce('Became an Other Lifecycle Date: ' + nullif(convert(varchar(10),BecameanOtherLifecycleDate,120),'') + char(10),'')
	, coalesce('Original Source Drill-Down 2: ' + nullif(OriginalSourceDrill_Down2,'') + char(10),'')
	, coalesce('Lifecycle Stage: ' + nullif(LifecycleStage,'') + char(10),'')
	, coalesce('Last Contacted: ' + nullif(convert(varchar(10),LastContacted,120),'') + char(10),'')
	, coalesce('Street Address: ' + nullif(StreetAddress,'') + char(10),'')
	, coalesce('Recent Conversion: ' + nullif(RecentConversion,'') + char(10),'')
	, coalesce('Country: ' + nullif(Country,'') + char(10),'')
	, coalesce('LinkedIn Connections: ' + nullif(convert(varchar(max),LinkedInConnections),'') + char(10),'')
	, coalesce('Persona: ' + nullif(Persona,'') + char(10),'')
	, coalesce('Salutation: ' + nullif(Salutation,'') + char(10),'')
	, coalesce('Sends Since Last Engagement: ' + nullif(convert(varchar(max),SendsSinceLastEngagement),'') + char(10),'')
	, coalesce('HubSpot Owner: ' + nullif(HubSpotOwner,'') + char(10),'')
	, coalesce('Became a Customer Date: ' + nullif(convert(varchar(10),BecameaCustomerDate,120),'') + char(10),'')
	, coalesce('Became a Lead Date: ' + nullif(convert(varchar(10),BecameaLeadDate,120),'') + char(10),'')
	, coalesce('Recent Sales Email Replied Date: ' + nullif(convert(varchar(10),RecentSalesEmailRepliedDate,120),'') + char(10),'')
	, coalesce('Website URL: ' + nullif(WebsiteURL,'') + char(10),'')
	, coalesce('Hub Spot Score: ' + nullif(convert(varchar(max),HubSpotScore),'') + char(10),'')
	, coalesce('Twitter Username: ' + nullif(TwitterUsername,'') + char(10),'')
	, coalesce('First Referring Site: ' + nullif(FirstReferringSite,'') + char(10),'')
	, coalesce('LastReferring Site: ' + nullif(LastReferringSite,'') + char(10),'')
	, coalesce('Twitter Profile Photo: ' + nullif(TwitterProfilePhoto,'') + char(10),'')
	, coalesce('Days To Close: ' + nullif(convert(varchar(max),DaysToClose),'') + char(10),'')
	, coalesce('Annual Revenue: ' + nullif(convert(varchar(max),AnnualRevenue),'') + char(10),'')
	, coalesce('Fax Number: ' + nullif(FaxNumber,'') + char(10),'')
	, coalesce('Industry: ' + nullif(Industry,'') + char(10),'')
	, coalesce('First email send date: ' + nullif(convert(varchar(10),Firstemailsenddate,120),'') + char(10),'')
	, coalesce('Email Address Quarantined: ' + nullif(EmailAddressQuarantined,'') + char(10),'')
	, coalesce('Number of Employees: ' + nullif(convert(varchar(max),NumberofEmployees),'') + char(10),'')
	, coalesce('Emails Bounced: ' + nullif(convert(varchar(max),EmailsBounced),'') + char(10),'')
	, coalesce('Associated Company: ' + nullif(AssociatedCompany,''),'')
	) as 'comment_content'
, getdate() as insert_timestamp
, -10 as assigned_user_id
, 1 as related_status
from AssignPRODContact apc
left join Contact c on c.ContactID = apc.AssignContactID

---4.2. Components to add for contact comments (65 rows)
--contact_id | user_id (-10) | comment_timestamp | comment_content | insert_timestamp | assigned_user_id (-10) | related_status (1)