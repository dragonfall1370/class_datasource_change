with dup as (select CompanyName as Company_Name from tblClient group by CompanyName having count(CompanyName) > 1),
file1 as (select ClientID as ID from tblDocument group by ClientID having count(*) > 1),
------------
sks as ( select tblClientSkillSet.ClientID as ClientID, tblSkillSet.SkillSet as SkillSet
from tblClientSkillSet left join tblSkillSet on tblClientSkillSet.SkillSetID = tblSkillSet.SkillSetID ),
------------
sksName as (
SELECT ClientID as 'ClientID',
    STUFF((SELECT DISTINCT ', ' + SkillSet
           FROM sks a 
           WHERE a.ClientID = b.ClientID 
          FOR XML PATH('')), 1, 2, '') as 'SkillName'
FROM sks b
GROUP BY ClientID ),
--------
document as (select docid, VacancyRef, ClientID, ContactID, CandidateID, Filename as 'Filename'
from tblDocument),
-----------------
docdup as 
(select document.ClientID as 'ClientID', document.Filename as 'FileName' from document where document.ClientID is not null),

---------------
docname as
(SELECT ClientID as 'ClientID',
    STUFF((SELECT DISTINCT ', ' + FileName
           FROM docdup a 
           WHERE a.ClientID = b.ClientID 
          FOR XML PATH('')), 1, 2, '') as 'FileName'
FROM docdup b
GROUP BY ClientID),
--------
ClientSiteLink as (select SiteID, SiteName, ClientID, AddressID, MainSwitchboard, strNotes from tblClientSites),
ManCon as (select tblConsultant.ContactID as 'ContactID', tblContact.LastName + ' ' + tblContact.FirstName as 'Name', tblEmailAddress.EmailAddress as 'Mail' 
from tblConsultant left join tblContact on tblConsultant.ContactID = tblContact.ContactID
left join tblEmailAddress on tblEmailAddress.ContactID = tblConsultant.ContactID),


----Main Script----------
company as (select tblClient.ClientID as 'company-externalId',
iif(tblClient.ManConsultantID = ManCon.ContactID,ManCon.Mail,'') as 'contact-owners',
tblClient.CompanyName as 'company-name',
iif(tblAddress.Building is null,'',
concat(nullif((trim(tblAddress.Building)),''),
nullif(', ' + (trim(tblAddress.Town)),', '),
nullif(', ' + (trim(tblAddress.PostalCode)),', '),
nullif((trim(tblAddress.Country)),''))) as 'company-locationAddress',
concat(
nullif(concat(tblAddress.County, ' '), ''),
nullif(concat(tblAddress.Town,' '),''),
nullif(concat(tblAddress.PostalCode,' '),''),
nullif(concat(tblAddress.Country,' '),''), 
(nullif(concat(', SwitchBoard: ',ClientSiteLink.MainSwitchboard),', SwitchBoard: ')))  as 'company-locationName',
iif(tblAddress.Town='' or tblAddress.Town is null,'',tblAddress.Town) as 'company-locationCity',
case when tblAddress.Country = 'UK' then 'GB'
when (tblAddress.Country = CTCode.Country_Name or tblAddress.Country = CTCode.Country_Code)
then CTCode.Country_Code else '' end as 'company-locationCountry',
iif(tblAddress.county='' or tblAddress.county is null,'',tblAddress.county) as 'company-locationDistrict',
iif(tblAddress.PostalCode='' or tblAddress.PostalCode is null,'',tblAddress.PostalCode) as 'company-locationZipCode',
iif(ClientSiteLink.MainSwitchboard='' or ClientSiteLink.MainSwitchboard is null,'',ClientSiteLink.MainSwitchboard) as 'company-switchBoard',
iif(tblClient.WebAddress='' or tblClient.WebAddress is null,'',tblClient.WebAddress) as 'company-website',
case when (tblClient.ClientID = docname.ClientID) then docname.FileName else '' end as 'company-document',
case when (tblClient.ClientID = ClientSiteLink.ClientID) then concat('Company External ID: ',tblClient.ClientID,char(13)+char(10),
'Site Name: ', ClientSiteLink.SiteName) else '' end as 'Company-note'
----company external ID for note ------


from tblClient left join ClientSiteLink on tblClient.ClientID = ClientSiteLink.ClientID
left join tblAddress on ClientSiteLink.AddressID =  tbladdress.id
left join tblLocation on tblAddress.LocationID = tblLocation.LocationID
left join CTCode on tblAddress.Country = CTCode.Country_Name
left join dup on tblClient.CompanyName = dup.Company_Name
left join jlc_tblClientStatus on tblClient.StatusEN = jlc_tblClientStatus.StatusID
left join jlc_tblSizes on tblClient.SizeEN = jlc_tblSizes.idSize
left join tblCurrency on tblClient.CurrID = tblCurrency.CurrID
left join tblClientFeePermanent on tblClient.ClientID = tblClientFeePermanent.ClientID
left join tblClientFeeContract on tblClient.ClientID = tblClientFeeContract.ClientID
left join tblClientFeeRebate on tblClient.ClientID = tblClientFeeRebate.ClientID
left join tblClientBusArea on tblClient.ClientID = tblClientBusArea.ClientID
left join tblBusinessArea on tblClientBusArea.BusAreaID = tblBusinessArea.BusAreaID
left join tblUser on tblClient.CreUser = tblUser.ContactID
left join sksName on tblClient.ClientID = sksName.ClientID
left join document on tblClient.ClientID = document.ClientID
left join ManCon on tblClient.ManConsultantID = ManCon.ContactID
left join docname on tblClient.ClientID = docname.ClientID
),

company2 as (select row_number() over (partition by [company-name] order by [company-externalId]) as 'com_num', * from company),

company3 as (select row_number() over (partition by [company-externalId] order by [company-externalId])  as 'id_num',* from company2 where com_num=1)

select iif(id_num<>1,([company-externalId] + id_num + 10000),[company-externalId])  as 'external_ID',* from company3 where com_num = 1





--Erodex LTD bug--

