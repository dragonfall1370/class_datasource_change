--DUPLICATION REGCONITION
with dup as (SELECT v.UniqueID
	, case when v.RoleDescription = '' then concat('JOB PLACEHOLDER',' - ', v.UniqueID) 
		else concat(case when v.JobCode is NULL or v.JobCode = '' then ''
		when replace(ct.Description,'<HIDE>','') <> '' then concat(ct.Description,' - ')
		else concat(v.JobCode,' - ') end,v.RoleDescription) end as RoleDescription
	, ROW_NUMBER() OVER(PARTITION BY lower(case when v.RoleDescription = '' then concat('JOB PLACEHOLDER',' - ', v.UniqueID) else v.RoleDescription end) ORDER BY v.UniqueID ASC) AS rn 
	from Vacancies v
	left join CodeTables ct on ct.Code = v.JobCode and ct.TabName = 'Role Codes')

, NewJobTitle as (select UniqueID
	, case when rn > 1 then concat(RoleDescription, ' - ', UniqueID)
	else RoleDescription end as NewRoleDescription
	from dup)

--MAIN SCRIPT
select case when v.ReportToUniqueID = 0 and v.SiteUniqID in (select SiteUniqueID from Sites) then concat('FR9999999',v.SiteUniqID)
	when v.ReportToUniqueID = 0 and v.SiteUniqID not in (select SiteUniqueID from Sites) then 'FR9999999'
	when v.ReportToUniqueID not in (select ContactUniqueID from Contacts) then 'FR9999999'
	else concat('FR',v.ReportToUniqueID) end as 'position-contactId'
, v.ReportToUniqueID as OriginalContactID
, v.SiteUniqID as OriginalCompanyID
, concat('FR',v.UniqueID) as 'position-externalId'
, n.NewRoleDescription as 'position-title'
, v.RoleDescription as OriginalJobTitle
, convert(date,v.CreationDate,120) as 'position-startDate'
, 'GBP' as 'position-currency'
, case when isnumeric(v.NoOfPositions) = 1 then v.NoOfPositions else 1 end as 'position-headcount'
, case v.MainConsultant
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
	end as 'position-owners'
, case v.VacancyType
	when 'PERM' then 'PERMANENT'
	when 'CONT' then 'CONTRACT'
	when 'TEMP' then 'TEMPORARY'
	when 'FTC' then 'CONTRACT'
	else '' end as 'position-type'
, stuff(coalesce(' '+ nullif(v.DocumentsNames001,''),'') + coalesce(',' + nullif(v.DocumentsNames002,''),'') 
	+ coalesce(',' + nullif(v.DocumentsNames003,''),'') + coalesce(',' + nullif(v.DocumentsNames004,''),''), 1, 1, '') as 'position-document'

--VACANCY NOTES
, concat_ws(char(10),concat('Vacancy External ID: ', v.UniqueID)
	-- , iif(v.AmendingUser = '' or v.AmendingUser is NULL,'',coalesce('Amending user: ' + case v.AmendingUser
	-- when 'ADM' then concat(v.AmendingUser, ' - ', 'Admin',' - ','No email adress - admin login')
	-- when 'ADMT' then concat(v.AmendingUser, ' - ', 'Staff Training Login')
	-- when 'AH' then concat(v.AmendingUser, ' - ', 'Alison Hart',' - ','alison@forwardrolerecruitment.com')
	-- when 'AM' then concat(v.AmendingUser, ' - ', 'Adam Miller',' - ','adam@forwardrolerecruitment.com')
	-- when 'ARAL' then concat(v.AmendingUser, ' - ', 'Arif Ali',' - ','arif@forwardrole.com')
	-- when 'BJ' then concat(v.AmendingUser, ' - ', 'Brian Johnson',' - ','brian@forwardrole.com')
	-- when 'BRTO' then concat(v.AmendingUser, ' - ', 'Brad Thomas',' - ','brad@forwardrole.com')
	-- when 'BS' then concat(v.AmendingUser, ' - ', 'Becky Smith',' - ','becky@forwardrole.com')
	-- when 'CP' then concat(v.AmendingUser, ' - ', 'Camilla Purdy',' - ','camilla@forwardrole.com (ADMIN) ')
	-- when 'CT' then concat(v.AmendingUser, ' - ', 'Chris Thomason',' - ','chris@forwardrolerecruitment.com')
	-- when 'DAHA' then concat(v.AmendingUser, ' - ', 'Danielle Harvey',' - ','danielle@forwardrolerecruitment.com')
	-- when 'DAHY' then concat(v.AmendingUser, ' - ', 'Dan Haydon',' - ','danh@forwardrole.com')
	-- when 'DANO' then concat(v.AmendingUser, ' - ', 'David Nottage',' - ','david@forwardrolerecruitment.com')
	-- when 'DETO' then concat(v.AmendingUser, ' - ', 'Desislava Torodova',' - ','desi@forwardrole.com')
	-- when 'DM' then concat(v.AmendingUser, ' - ', 'Dan Middlebrook',' - ','dan@forwardrole.com')
	-- when 'DS' then concat(v.AmendingUser, ' - ', 'Dominic Scales',' - ','dominic@forwardrolerecruitment.com')
	-- when 'EMME' then concat(v.AmendingUser, ' - ', 'Emma Melling',' - ','emma@forwardrole.com')
	-- when 'GRBO' then concat(v.AmendingUser, ' - ', 'Grant Bodie',' - ','grant@forwardrole.com')
	-- when 'GW' then concat(v.AmendingUser, ' - ', 'Guy Walker',' - ','guy@forwardrole.com')
	-- when 'HB' then concat(v.AmendingUser, ' - ', 'Henna Baig',' - ','henna@forwardrolerecruitment.com')
	-- when 'HC' then concat(v.AmendingUser, ' - ', 'Helen Colley',' - ','helen@forwardrole.com')
	-- when 'IL' then concat(v.AmendingUser, ' - ', 'Ian Lenahan',' - ','ian@forwardrolerecruitment.com')
	-- when 'IZKH' then concat(v.AmendingUser, ' - ', 'Izzy Khan',' - ','izzy@forwardrole.com')
	-- when 'JH' then concat(v.AmendingUser, ' - ', 'Jack Harrison',' - ','jack@forwardrolerecruitment.com')
	-- when 'JOPE' then concat(v.AmendingUser, ' - ', 'Josh Pepper',' - ','josh@forwardrole.com')
	-- when 'JS' then concat(v.AmendingUser, ' - ', 'Jon Saxon',' - ','jon@forwardrolerecruitment.com')
	-- when 'KM' then concat(v.AmendingUser, ' - ', 'Katrina Mvafferty',' - ','katrina@forwardrolerecruitment.com')
	-- when 'LK' then concat(v.AmendingUser, ' - ', 'Lucy Ketley',' - ','lucy@forwardrolerecruitment.com')
	-- when 'MABO' then concat(v.AmendingUser, ' - ', 'Matthew Borthwick',' - ','mattb@forwardrole.com')
	-- when 'MD' then concat(v.AmendingUser, ' - ', 'Matt Darwell',' - ','matt@forwardrole.com')
	-- when 'MIRH' then concat(v.AmendingUser, ' - ', 'Mike Rhodes',' - ','mike@forwardrole.com')
	-- when 'NAYO' then concat(v.AmendingUser, ' - ', 'Nathan Young',' - ','nathan@forwardrole.com')
	-- when 'PAMC' then concat(v.AmendingUser, ' - ', 'Patrick McMahon',' - ','patrick@forwardrole.com')
	-- when 'PAWE' then concat(v.AmendingUser, ' - ', 'Paddy Wells',' - ','paddy@forwardrole.com')
	-- when 'PHST' then concat(v.AmendingUser, ' - ', 'Phill Stott',' - ','phill@forwardrole.com')
	-- when 'RADA' then concat(v.AmendingUser, ' - ', 'Rachel Davies',' - ','rachel@forwardrole.com')
	-- when 'RAWH' then concat(v.AmendingUser, ' - ', 'Rachel Wheeler',' - ','rachelw@forwardrole.com')
	-- when 'RF' then concat(v.AmendingUser, ' - ', 'Ricardo Favhin')
	-- when 'RYDO' then concat(v.AmendingUser, ' - ', 'Ryan Dolan',' - ','ryan@forwardrole.com')
	-- when 'SASH' then concat(v.AmendingUser, ' - ', 'Sam Shinners',' - ','sam@forwardrolere.com')
	-- when 'SOPA' then concat(v.AmendingUser, ' - ', 'Sophie Page',' - ','sophie@forwardrole.com')
	-- when 'ST' then concat(v.AmendingUser, ' - ', 'Steve Thompson',' - ','steve@forwardrole.com')
	-- when 'TOBY' then concat(v.AmendingUser, ' - ', 'Tom Byrne',' - ','tom@forwardrole.com')
	-- when 'TP' then concat(v.AmendingUser, ' - ', 'Thea Parry',' - ','thea@forwardrolerecruitment.com')
	-- when 'WIVE' then concat(v.AmendingUser, ' - ', 'Will Velios',' - ','will@forwardrolerecruitment.com')
	-- else v.AmendingUser end, ''))
	-- , iif(v.AmendmentDate is NULL,'',concat('Amendment Date: ', convert(varchar(10),v.AmendmentDate,120))) --| removed on 15052018
	, iif(v.CreationDate is NULL,NULL,concat('Influence Creation Date: ', convert(varchar(10),v.CreationDate,120)))
	, iif(v.GeneratedReference is NULL,NULL,concat('Influence VR Code: ', v.GeneratedReference))
	, iif(v.JobCode is NULL,NULL,concat('Job Code: ', v.JobCode))
	, iif(v.FromSalary is NULL,NULL,concat('From Salary: ', v.FromSalary)) --NEED CUSTOM SCRIPT
	, iif(v.ToSalary is NULL,NULL,concat('To Salary: ', v.ToSalary)) --NEED CUSTOM SCRIPT
	-- , iif(v.FromOTE is NULL,NULL,concat('From OTE: ', v.FromOTE)) --| removed on 17052018
	-- , iif(v.ToOTE is NULL,NULL,concat('To OTE: ', v.ToOTE)) --| removed on 17052018
	, iif(v.BusinessType = '' or v.BusinessType is NULL,NULL,concat('Business Type: ', v.BusinessType, ' - ', ct.Description))
	, iif(v.Status = '' or v.Status is NULL,NULL,concat('Status: ', v.Status, ' - ', ct2.Description))
	-- , iif(v.TermsValue = '' or v.TermsValue is NULL,NULL,concat('Terms Value: ', v.TermsValue)) --| removed on 17052018
	, iif(v.ChargeRate1 = '' or v.ChargeRate1 is NULL,NULL,concat('Charge Rate: ', v.ChargeRate1))
	, iif(v.PayRate1 = '' or v.PayRate1 is NULL,NULL,concat('Pay Rate: ', v.PayRate1))
	, iif(v.DatePlaced = '' or v.DatePlaced is NULL,NULL,concat('DatePlaced: ', convert(varchar(10),v.DatePlaced,120)))
	, iif(v.StageReached = '' or v.StageReached is NULL,NULL,concat('Stage Reached: ', v.StageReached, ' - ', ct3.Description))
	, iif(v.Source = '' or v.Source is NULL,NULL,concat('Code: ', v.Source, ' - ', ct4.Description))
	, iif(ev.Role_Text_Job_Spec = '' or ev.Role_Text_Job_Spec is NULL,NULL,concat('Code: ', ev.Role_Text_Job_Spec))
	, iif(ev.Vacancy_Notes = '' or ev.Vacancy_Notes is NULL,NULL,concat('Code: ', ev.Vacancy_Notes))
	, iif(ev.Person_Notes = '' or ev.Person_Notes is NULL,NULL,concat('Code: ', ev.Person_Notes))
	, iif(ev.Qualification_Notes = '' or ev.Qualification_Notes is NULL,NULL,concat('Code: ', ev.Qualification_Notes))
	, iif(ev.Contract_Notes = '' or ev.Contract_Notes is NULL,NULL,concat('Code: ', ev.Contract_Notes))
	, iif(ev.Interview_Notes = '' or ev.Interview_Notes is NULL,NULL,concat('Code: ', ev.Interview_Notes))
	, iif(ev.Advertising_Text = '' or ev.Advertising_Text is NULL,NULL,concat('Code: ', ev.Advertising_Text))
	) as 'position-note'
from Vacancies v
left join NewJobTitle n on n.UniqueID = v.UniqueID
left join CodeTables ct on ct.Code = v.BusinessType and ct.TabName = 'Bus Type'
left join CodeTables ct2 on ct2.Code = v.Status and ct2.TabName = 'Vac Status Code'
left join CodeTables ct3 on ct3.Code = v.StageReached and ct3.TabName = 'Match Status'
left join CodeTables ct4 on ct4.Code COLLATE SQL_Latin1_General_CP1_CS_AS  = v.Source and ct4.TabName = 'Vacancy Source Co'
left join ENIVTAB0001 ev on ev.Unique_ID = v.UniqueID
--where v.Source = 'Ref' COLLATE SQL_Latin1_General_CP1_CS_AS | to search for case-sensitive keyword
order by v.UniqueID