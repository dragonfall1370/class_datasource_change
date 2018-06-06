--CONTACTS WORK FOR > 1 COMPANIES
with 
dupcontact as (select ContactUniqueID, ROW_NUMBER() OVER(PARTITION BY ContactUniqueID ORDER BY SiteUnique ASC) AS rn 
	from Contacts)
--No same contact works for more than 1 companies

--CONTACT MAIL DUPLICATION
, EmailDupRegconition as (select ContactUniqueID, replace(replace(replace(replace(replace(replace(Email,': ',''),':',''),'   ',''),'/ ',''),'/',''),' ','') as Email
	, ROW_NUMBER() OVER(PARTITION BY Email ORDER BY ContactUniqueID ASC) AS rn 
	from Contacts
	where Email <> ''
	and Email like '%_@_%.__%')

, ContactEmail as (select ContactUniqueID, rn
	, case when rn = 1 then Email else concat(rn,'_',Email) end as Email
	from EmailDupRegconition)

--CONTACTS PLACEHOLDER FROM VACANCY
, ContactPlaceholder as (select distinct v.SiteUniqID, v.ReportToUniqueID
	, concat(9999999,v.SiteUniqID) as ContactPlaceholderID
	, 'DEFAULT CONTACT' as ContactPlaceholderFirstName
	, concat(' - ',s.SiteUniqueID) as ContactPlaceHolderLastName
	from Vacancies v
	left join Sites s on s.SiteUniqueID = v.SiteUniqID
	where v.ReportToUniqueID = 0
	and v.SiteUniqID in (select SiteUniqueID from Sites))

--MAIN SCRIPT
select case when cc.SiteUnique = '' or cc.SiteUnique is NULL then 'FR9999999'
	when cc.SiteUnique not in (select SiteUniqueID from Sites) then 'FR9999999' --some sites were removed from Sites
	else concat('FR',cc.SiteUnique) as 'contact-companyId'
, cc.SiteUnique as OriginalCompanyID
, concat('FR', cc.ContactUniqueID) as 'contact-externalId'
, coalesce(nullif(cc.Forename,''),concat('Firstname - ',cc.ContactUniqueID)) as 'contact-firstName'
, coalesce(nullif(cc.Surname,''),concat('Lastname - ',cc.ContactUniqueID)) as 'contact-lastName'
, ce.Email as 'contact-email'
, ltrim(Stuff(coalesce(' ' + NULLIF(cc.Telephone, ''), '')
              + coalesce(',' + NULLIF(cc.Extension, ''), '')
			  + coalesce(',' + NULLIF(cc.DDI, ''), '')
                , 1, 1, '')) as 'contact-phone'
, coalesce(cc.Mobile,NULL) as 'contact-mobile' --CUSTOM SCRIPT
, left(nullif(cc.SocialNetSite1,''),200) as 'contact-linkedin'
, iif(cc.PositionCode is NULL or cc.PositionCode = '',NULL,iif(ct.Description = '<HIDE>',NULL,ct.Description)) as 'contact-jobTitle'
, cc.PositionCode
, concat_ws(char(10),concat('Contact External ID: ', cc.ContactUniqueID)
	, iif(cc.SiteUnique = '' or cc.SiteUnique is NULL,NULL,concat('Site unique: ', cc.SiteUnique,' - ', s.Organisation))
	-- , iif(cc.AmendingUser = '' or cc.AmendingUser is NULL,'',coalesce(char(10) + 'Amending user: ' + case cc.AmendingUser
	-- when 'ADM' then concat(cc.AmendingUser, ' - ', 'Admin',' - ','No email adress - admin login')
	-- when 'ADMT' then concat(cc.AmendingUser, ' - ', 'Staff Training Login')
	-- when 'AH' then concat(cc.AmendingUser, ' - ', 'Alison Hart',' - ','alison@forwardrolerecruitment.com')
	-- when 'AM' then concat(cc.AmendingUser, ' - ', 'Adam Miller',' - ','adam@forwardrolerecruitment.com')
	-- when 'ARAL' then concat(cc.AmendingUser, ' - ', 'Arif Ali',' - ','arif@forwardrole.com')
	-- when 'BJ' then concat(cc.AmendingUser, ' - ', 'Brian Johnson',' - ','brian@forwardrole.com')
	-- when 'BRTO' then concat(cc.AmendingUser, ' - ', 'Brad Thomas',' - ','brad@forwardrole.com')
	-- when 'BS' then concat(cc.AmendingUser, ' - ', 'Becky Smith',' - ','becky@forwardrole.com')
	-- when 'CP' then concat(cc.AmendingUser, ' - ', 'Camilla Purdy',' - ','camilla@forwardrole.com (ADMIN) ')
	-- when 'CT' then concat(cc.AmendingUser, ' - ', 'Chris Thomason',' - ','chris@forwardrolerecruitment.com')
	-- when 'DAHA' then concat(cc.AmendingUser, ' - ', 'Danielle Harvey',' - ','danielle@forwardrolerecruitment.com')
	-- when 'DAHY' then concat(cc.AmendingUser, ' - ', 'Dan Haydon',' - ','danh@forwardrole.com')
	-- when 'DANO' then concat(cc.AmendingUser, ' - ', 'David Nottage',' - ','david@forwardrolerecruitment.com')
	-- when 'DETO' then concat(cc.AmendingUser, ' - ', 'Desislava Torodova',' - ','desi@forwardrole.com')
	-- when 'DM' then concat(cc.AmendingUser, ' - ', 'Dan Middlebrook',' - ','dan@forwardrole.com')
	-- when 'DS' then concat(cc.AmendingUser, ' - ', 'Dominic Scales',' - ','dominic@forwardrolerecruitment.com')
	-- when 'EMME' then concat(cc.AmendingUser, ' - ', 'Emma Melling',' - ','emma@forwardrole.com')
	-- when 'GRBO' then concat(cc.AmendingUser, ' - ', 'Grant Bodie',' - ','grant@forwardrole.com')
	-- when 'GW' then concat(cc.AmendingUser, ' - ', 'Guy Walker',' - ','guy@forwardrole.com')
	-- when 'HB' then concat(cc.AmendingUser, ' - ', 'Henna Baig',' - ','henna@forwardrolerecruitment.com')
	-- when 'HC' then concat(cc.AmendingUser, ' - ', 'Helen Colley',' - ','helen@forwardrole.com')
	-- when 'IL' then concat(cc.AmendingUser, ' - ', 'Ian Lenahan',' - ','ian@forwardrolerecruitment.com')
	-- when 'IZKH' then concat(cc.AmendingUser, ' - ', 'Izzy Khan',' - ','izzy@forwardrole.com')
	-- when 'JH' then concat(cc.AmendingUser, ' - ', 'Jack Harrison',' - ','jack@forwardrolerecruitment.com')
	-- when 'JOPE' then concat(cc.AmendingUser, ' - ', 'Josh Pepper',' - ','josh@forwardrole.com')
	-- when 'JS' then concat(cc.AmendingUser, ' - ', 'Jon Saxon',' - ','jon@forwardrolerecruitment.com')
	-- when 'KM' then concat(cc.AmendingUser, ' - ', 'Katrina McCafferty',' - ','katrina@forwardrolerecruitment.com')
	-- when 'LK' then concat(cc.AmendingUser, ' - ', 'Lucy Ketley',' - ','lucy@forwardrolerecruitment.com')
	-- when 'MABO' then concat(cc.AmendingUser, ' - ', 'Matthew Borthwick',' - ','mattb@forwardrole.com')
	-- when 'MD' then concat(cc.AmendingUser, ' - ', 'Matt Darwell',' - ','matt@forwardrole.com')
	-- when 'MIRH' then concat(cc.AmendingUser, ' - ', 'Mike Rhodes',' - ','mike@forwardrole.com')
	-- when 'NAYO' then concat(cc.AmendingUser, ' - ', 'Nathan Young',' - ','nathan@forwardrole.com')
	-- when 'PAMC' then concat(cc.AmendingUser, ' - ', 'Patrick McMahon',' - ','patrick@forwardrole.com')
	-- when 'PAWE' then concat(cc.AmendingUser, ' - ', 'Paddy Wells',' - ','paddy@forwardrole.com')
	-- when 'PHST' then concat(cc.AmendingUser, ' - ', 'Phill Stott',' - ','phill@forwardrole.com')
	-- when 'RADA' then concat(cc.AmendingUser, ' - ', 'Rachel Davies',' - ','rachel@forwardrole.com')
	-- when 'RAWH' then concat(cc.AmendingUser, ' - ', 'Rachel Wheeler',' - ','rachelw@forwardrole.com')
	-- when 'RF' then concat(cc.AmendingUser, ' - ', 'Ricardo Facchin')
	-- when 'RYDO' then concat(cc.AmendingUser, ' - ', 'Ryan Dolan',' - ','ryan@forwardrole.com')
	-- when 'SASH' then concat(cc.AmendingUser, ' - ', 'Sam Shinners',' - ','sam@forwardrolere.com')
	-- when 'SOPA' then concat(cc.AmendingUser, ' - ', 'Sophie Page',' - ','sophie@forwardrole.com')
	-- when 'ST' then concat(cc.AmendingUser, ' - ', 'Steve Thompson',' - ','steve@forwardrole.com')
	-- when 'TOBY' then concat(cc.AmendingUser, ' - ', 'Tom Byrne',' - ','tom@forwardrole.com')
	-- when 'TP' then concat(cc.AmendingUser, ' - ', 'Thea Parry',' - ','thea@forwardrolerecruitment.com')
	-- when 'WIVE' then concat(cc.AmendingUser, ' - ', 'Will Velios',' - ','will@forwardrolerecruitment.com')
	-- else cc.AmendingUser end, '')) --| removed on 15052018
	-- , iif(cc.AmendmentDate is NULL,'',concat(char(10), 'Amendment Date: ', convert(varchar(10),cc.AmendmentDate,120))) --| removed on 15052018
	, iif(cc.CreationDate is NULL,NULL,concat('Influence created date: ', convert(varchar(10),cc.CreationDate,120)))
	-- , iif(cc.ImportanceInOrg = '' or cc.ImportanceInOrg is NULL,NULL,concat('Importance In Org: ', cc.ImportanceInOrg, ' - ',ct2.Description)) --| removed on 17052018
	--, iif(cc.Initials = '' or cc.Initials is NULL,NULL,concat('Initials: ', cc.Initials))
	, iif(cc.Title = '' or cc.Title is NULL,NULL,concat('Title: ', cc.Title))
	-- , iif(cc.WebAddress = '' or cc.WebAddress is NULL,NULL,concat('Web Address: ', cc.WebAddress)) --| removed on 15052018
	, iif(cc.ReportsTo = 0 or cc.ReportsTo is NULL,NULL,concat('Reports To: ', c2.Forename, ' ', c2.Surname))
	-- , iif(cc.EshotYN = '' or cc.EshotYN is NULL,NULL,concat('EshotYN: ', cc.EshotYN)) --| NEED JECTION updated on 16052018
	, coalesce('Contact Notes: ' + nullif(cm._Contact_Notes_,''),'')
	) as 'contact-note'
-- from (select *
	-- , case ImportanceInOrg
		-- when '1' then '01'
		-- when '2' then '02'
		-- when '9' then '09'
		-- else ImportanceInOrg end as ImportanceInOrg2
		-- from Contacts) as cc
from Contacts as cc --already removed ImportanceInOrg
left join Sites s on s.SiteUniqueID = cc.SiteUnique
left join ContactEmail ce on ce.ContactUniqueID = cc.ContactUniqueID
left join CodeTables ct on ct.Code = cc.PositionCode and ct.TabName = 'Role Codes'
-- left join CodeTables ct2 on ct2.Code = cc.ImportanceInOrg and ct2.TabName = 'CMS Importance' --| removed on 17052018
left join Contacts c2 on c2.ContactUniqueID = cc.ReportsTo
left join CMCONT0001 cm on cm._Contact_Unique_ID_ = cc.ContactUniqueID

UNION ALL

select concat('FR',SiteUniqID)
, SiteUniqID as OriginalCompanyID
, concat('FR',ContactPlaceholderID)
, ContactPlaceholderFirstName
, ContactPlaceHolderLastName, '', '', '', '', '', '', 'This is Contact Placeholder from Vacancy'
from ContactPlaceholder

UNION ALL

select 'FR9999999',9999999,'FR9999999','DEFAULT','CONTACT','', '', '', '', '', '', 'This is default contact placeholder from data import'