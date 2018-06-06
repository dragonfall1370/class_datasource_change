/* 
, iif(s.SiteUniqueID in (select SiteUniqueID from dup where dup.rn > 1)
	, iif(dup.Organisation = '' or dup.Organisation is null, concat('No Company Name - ', dup.SiteUniqueID), concat(dup.Organisation,' - ',iif(dup.SiteAddressLine6 in ('',' '),convert(varchar(max),dup.SiteUniqueID),dup.SiteAddressLine6)))
	, concat(s.Organisation,' - ', s.SiteAddressLine6)) as 'company-name'

*/

---COMPANY LOCATION
with 
--loc as (
--	select SiteUniqueID, SiteAddressHseNo, SiteAddressLine1, SiteAddressLine2, SiteAddressLine3
--			, SiteAddressLine4, SiteAddressLine5, SiteAddressLine6, SitePostcode, Locality
--			, coalesce(ltrim(Stuff(
--			  Coalesce(' ' + NULLIF(SiteAddressHseNo, ''), '')
--			+ Coalesce(', ' + NULLIF(SiteAddressLine1, ''), '')
--			+ Coalesce(', ' + NULLIF(SiteAddressLine2, ''), '')
--			+ Coalesce(', ' + NULLIF(SiteAddressLine3, ''), '')
--			+ Coalesce(', ' + NULLIF(SiteAddressLine4, ''), '')
--			+ Coalesce(', ' + NULLIF(SiteAddressLine5, ''), '')
--			+ Coalesce(', ' + NULLIF(SiteAddressLine6, ''), '')
--			+ Coalesce(', ' + NULLIF(SitePostcode, ''), '')
--			, 1, 1, '')), Locality) as 'locationName'
--	from Sites)

---DUPLICATION REGCONITION
dup as (SELECT SiteUniqueID, Organisation, SiteAddressLine6, ROW_NUMBER() OVER(PARTITION BY Organisation ORDER BY SiteUniqueID ASC) AS rn, ROW_NUMBER() OVER(PARTITION BY Organisation ORDER BY SiteAddressLine6 DESC) AS Cityrn
FROM Sites)

--select * from dup
--where rn > 1 and Cityrn > 1

---MAIN SCRIPT
select
  concat('FR', s.SiteUniqueID) as 'company-externalId'
, s.Organisation as '(OriginalName)'
, case when s.SiteUniqueID in (select SiteUniqueID from dup where dup.rn > 1) and s.SiteUniqueID in (select SiteUniqueID from dup where dup.Cityrn = 1) and dup.SiteAddressLine6 not in ('',' ') then concat(dup.Organisation, ' - ', dup.SiteAddressLine6)
	when s.SiteUniqueID in (select SiteUniqueID from dup where dup.rn > 1) and s.SiteUniqueID in (select SiteUniqueID from dup where dup.Cityrn > 1) and dup.SiteAddressLine6 not in ('',' ') then concat(dup.Organisation, ' - ', dup.SiteAddressLine6, ' - ', dup.Cityrn)
	when s.SiteUniqueID in (select SiteUniqueID from dup where dup.rn > 1) and (dup.Organisation = '' or dup.Organisation is null) then concat('No Company Name - ', dup.SiteUniqueID)
	when s.SiteUniqueID in (select SiteUniqueID from dup where dup.rn > 1) and dup.SiteAddressLine6 in ('',' ') then concat(dup.Organisation, ' - ', convert(varchar(max),dup.SiteUniqueID))
	else concat(s.Organisation,coalesce(' - ' + nullif(s.SiteAddressLine6,''),'')) end as 'company-name'
, case c.AccountManager
	when 'AH' then 'alison@forwardrolerecruitment.com'
	when 'AM' then 'adam@forwardrolerecruitment.com'
	when 'ARAL' then 'arif@forwardrole.com'
	when 'BJ' then 'brian@forwardrole.com'
	when 'BRTO' then 'brad@forwardrole.com'
	when 'BS' then 'becky@forwardrole.com'
	when 'CP' then 'camilla@forwardrole.com'
	when 'CT' then 'chris@forwardrolerecruitment.com'
	when 'DAHA' then 'danielle@forwardrolerecruitment.com'
	when 'DAHY' then 'danh@forwardrole.com'
	when 'DANO' then 'david@forwardrolerecruitment.com'
	when 'DETO' then 'desi@forwardrole.com'
	when 'DM' then 'dan@forwardrole.com'
	when 'DS' then 'dominic@forwardrolerecruitment.com'
	when 'EMME' then 'emma@forwardrole.com'
	when 'GRBO' then 'grant@forwardrole.com'
	when 'GW' then 'guy@forwardrole.com'
	when 'HB' then 'henna@forwardrolerecruitment.com'
	when 'HC' then 'helen@forwardrole.com'
	when 'IL' then 'ian@forwardrolerecruitment.com'
	when 'IZKH' then 'izzy@forwardrole.com'
	when 'JH' then 'jack@forwardrolerecruitment.com'
	when 'JOPE' then 'josh@forwardrole.com'
	when 'JS' then 'jon@forwardrolerecruitment.com'
	when 'KM' then 'katrina@forwardrolerecruitment.com'
	when 'LK' then 'lucy@forwardrolerecruitment.com'
	when 'MABO' then 'mattb@forwardrole.com'
	when 'MD' then 'matt@forwardrole.com'
	when 'MIRH' then 'mike@forwardrole.com'
	when 'NAYO' then 'nathan@forwardrole.com'
	when 'PAMC' then 'patrick@forwardrole.com'
	when 'PAWE' then 'paddy@forwardrole.com'
	when 'PHST' then 'phill@forwardrole.com'
	when 'RADA' then 'rachel@forwardrole.com'
	when 'RAWH' then 'rachelw@forwardrole.com'
	when 'RYDO' then 'ryan@forwardrole.com'
	when 'SASH' then 'sam@forwardrolere.com'
	when 'SOPA' then 'sophie@forwardrole.com'
	when 'ST' then 'steve@forwardrole.com'
	when 'TOBY' then 'tom@forwardrole.com'
	when 'TP' then 'thea@forwardrolerecruitment.com'
	when 'WIVE' then 'will@forwardrolerecruitment.com'
	end as 'company-owners'
, s.SiteAddress as 'company-locationName'
, s.SiteAddress as 'company-locationAddress'
, nullif(s.SiteAddressLine6,'') as 'company-locationCity'
, nullif(s.Locality,'') as 'company-locationDistrict'
, nullif(s.SitePostcode,'') as 'company-locationZipCode'
, nullif(s.SitePhoneNumber,'') as 'company-phone'
, iif(s.WebAddress like '%.%',left(s.WebAddress,100),NULL) as 'company-website'
, stuff(coalesce(' '+ c.DocumentsNames001,NULL) + coalesce(',' + c.DocumentsNames002,NULL) + coalesce(',' + c.DocumentsNames003,NULL), 1, 1, '') as 'company-document'
, concat('Company External ID: ', s.SiteUniqueID,char(10)
			, iif(s.AccountCode = '' or s.AccountCode is NULL,'',concat(char(10), 'Account Code: ', s.AccountCode))
			, iif(c.MainSiteUnique = '' or c.MainSiteUnique is NULL,'',concat(char(10), 'Main Site Unique: ', c.MainSiteUnique))
			--, iif(s.AmendingUser = '' or s.AmendingUser is NULL,'',concat(char(10), 'Amending User: ', s.AmendingUser)) --| removed on 15052018
			--, iif(s.AmendmentDate is NULL,'',concat(char(10), 'Amendment Date: ', convert(varchar(10),s.AmendmentDate,120))) --| removed on 15052018
			, iif(s.CreationDate is NULL,'',concat(char(10), 'Influence Created Date: ', convert(varchar(10),s.CreationDate,120)))
			--, iif(s.Email = '' or s.Email is NULL,'',concat(char(10), 'Email: ', s.Email)) --| removed on 17052018
			--, iif(s.MarkedForDeletion = '' or s.MarkedForDeletion is NULL,'',concat(char(10), 'Marked for deletion: ', s.MarkedForDeletion)) --| removed on 15052018
			--, iif(s.MailshotYN = '' or s.MailshotYN is NULL,'',concat(char(10), 'Mailshot YN: ', s.MailshotYN)) --| removed on 15052018
			, iif(c.BusinessType001 = '' or c.BusinessType001 is NULL,'',concat(char(10), 'Business Type: ', c.BusinessType001, ' - ', ct4.Description))
			--, iif(c.EnquirySource = '' or c.EnquirySource is NULL,'',Concat(char(10), 'Enquiry Source: ', c.EnquirySource, ' - ', ct3.Description))
			--, iif(c.ClientImportance = '' or c.ClientImportance is NULL,'',Concat(char(10), 'Client Importance: ', c.ClientImportance, ' - ', ct2.Description)) --| removed on 15052018
			--, iif(c.ClientStatus = '' or c.ClientStatus is NULL,'',Concat(char(10), 'Client Status: ', c.ClientStatus, ' - ', ct.Description)) --| removed on 15052018
			) as 'company-note'
FROM Sites s
	left join Clients c on s.SiteUniqueID = c.MainSiteUnique
	left join dup on s.SiteUniqueID = dup.SiteUniqueID
	--left join CodeTables ct on ct.Code = c.ClientStatus and ct.TabName = 'Client Status' --| removed on 15052018
	--left join CodeTables ct2 on ct2.Code = c.ClientImportance and ct2.TabName = 'Client Importance' --| removed on 15052018
	--left join CodeTables ct3 on ct3.Code = c.EnquirySource and ct3.TabName = 'Client Enq Src'
	left join CodeTables ct4 on ct4.Code = c.BusinessType001 and ct4.TabName = 'Bus Type'
	--order by s.SiteUniqueID

UNION ALL

select 'FR9999999','','Default Company','','','','','','','','','','This is Default Company from Data Import'