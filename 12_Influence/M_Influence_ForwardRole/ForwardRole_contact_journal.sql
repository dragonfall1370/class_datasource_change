---CONTACT JOURNALS
select j.UniqueID
, j.Subject
, j.ClientUniqueID
, j.SiteUniqueID
, j.CandidateUniqueID
, concat('FR', j.ContactUniqueID) as Forward_ContactExtID
, j.VacancyUniqueID
, concat_ws(char(10)
	, coalesce('Journal ID: ' + convert(varchar(10),j.UniqueID),'')
	, coalesce('Call type: ' + j.CallType + ' - ' + ct.Description,'')
	, coalesce('Contact type: ' + j.ContactType + ' - ' + ct2.Description,'')
	, coalesce('Creating user: ' + case j.CreatingUser
	when 'ADM' then concat(j.CreatingUser, ' - ', 'Admin',' - ','No email adress - admin login')
	when 'ADMT' then concat(j.CreatingUser, ' - ', 'Staff Training Login')
	when 'AH' then concat(j.CreatingUser, ' - ', 'Alison Hart',' - ','alison@forwardrolerecruitment.com')
	when 'AM' then concat(j.CreatingUser, ' - ', 'Adam Miller',' - ','adam@forwardrolerecruitment.com')
	when 'ARAL' then concat(j.CreatingUser, ' - ', 'Arif Ali',' - ','arif@forwardrole.com')
	when 'BJ' then concat(j.CreatingUser, ' - ', 'Brian Johnson',' - ','brian@forwardrole.com')
	when 'BRTO' then concat(j.CreatingUser, ' - ', 'Brad Thomas',' - ','brad@forwardrole.com')
	when 'BS' then concat(j.CreatingUser, ' - ', 'Becky Smith',' - ','becky@forwardrole.com')
	when 'CP' then concat(j.CreatingUser, ' - ', 'Camilla Purdy',' - ','camilla@forwardrole.com (ADMIN) ')
	when 'CT' then concat(j.CreatingUser, ' - ', 'Chris Thomason',' - ','chris@forwardrolerecruitment.com')
	when 'DAHA' then concat(j.CreatingUser, ' - ', 'Danielle Harvey',' - ','danielle@forwardrolerecruitment.com')
	when 'DAHY' then concat(j.CreatingUser, ' - ', 'Dan Haydon',' - ','danh@forwardrole.com')
	when 'DANO' then concat(j.CreatingUser, ' - ', 'David Nottage',' - ','david@forwardrolerecruitment.com')
	when 'DETO' then concat(j.CreatingUser, ' - ', 'Desislava Torodova',' - ','desi@forwardrole.com')
	when 'DM' then concat(j.CreatingUser, ' - ', 'Dan Middlebrook',' - ','dan@forwardrole.com')
	when 'DS' then concat(j.CreatingUser, ' - ', 'Dominic Scales',' - ','dominic@forwardrolerecruitment.com')
	when 'EMME' then concat(j.CreatingUser, ' - ', 'Emma Melling',' - ','emma@forwardrole.com')
	when 'GRBO' then concat(j.CreatingUser, ' - ', 'Grant Bodie',' - ','grant@forwardrole.com')
	when 'GW' then concat(j.CreatingUser, ' - ', 'Guy Walker',' - ','guy@forwardrole.com')
	when 'HB' then concat(j.CreatingUser, ' - ', 'Henna Baig',' - ','henna@forwardrolerecruitment.com')
	when 'HC' then concat(j.CreatingUser, ' - ', 'Helen Colley',' - ','helen@forwardrole.com')
	when 'IL' then concat(j.CreatingUser, ' - ', 'Ian Lenahan',' - ','ian@forwardrolerecruitment.com')
	when 'IZKH' then concat(j.CreatingUser, ' - ', 'Izzy Khan',' - ','izzy@forwardrole.com')
	when 'JH' then concat(j.CreatingUser, ' - ', 'Jack Harrison',' - ','jack@forwardrolerecruitment.com')
	when 'JOPE' then concat(j.CreatingUser, ' - ', 'Josh Pepper',' - ','josh@forwardrole.com')
	when 'JS' then concat(j.CreatingUser, ' - ', 'Jon Saxon',' - ','jon@forwardrolerecruitment.com')
	when 'KM' then concat(j.CreatingUser, ' - ', 'Katrina McCafferty',' - ','katrina@forwardrolerecruitment.com')
	when 'LK' then concat(j.CreatingUser, ' - ', 'Lucy Ketley',' - ','lucy@forwardrolerecruitment.com')
	when 'MABO' then concat(j.CreatingUser, ' - ', 'Matthew Borthwick',' - ','mattb@forwardrole.com')
	when 'MD' then concat(j.CreatingUser, ' - ', 'Matt Darwell',' - ','matt@forwardrole.com')
	when 'MIRH' then concat(j.CreatingUser, ' - ', 'Mike Rhodes',' - ','mike@forwardrole.com')
	when 'NAYO' then concat(j.CreatingUser, ' - ', 'Nathan Young',' - ','nathan@forwardrole.com')
	when 'PAMC' then concat(j.CreatingUser, ' - ', 'Patrick McMahon',' - ','patrick@forwardrole.com')
	when 'PAWE' then concat(j.CreatingUser, ' - ', 'Paddy Wells',' - ','paddy@forwardrole.com')
	when 'PHST' then concat(j.CreatingUser, ' - ', 'Phill Stott',' - ','phill@forwardrole.com')
	when 'RADA' then concat(j.CreatingUser, ' - ', 'Rachel Davies',' - ','rachel@forwardrole.com')
	when 'RAWH' then concat(j.CreatingUser, ' - ', 'Rachel Wheeler',' - ','rachelw@forwardrole.com')
	when 'RF' then concat(j.CreatingUser, ' - ', 'Ricardo Facchin')
	when 'RYDO' then concat(j.CreatingUser, ' - ', 'Ryan Dolan',' - ','ryan@forwardrole.com')
	when 'SASH' then concat(j.CreatingUser, ' - ', 'Sam Shinners',' - ','sam@forwardrolere.com')
	when 'SOPA' then concat(j.CreatingUser, ' - ', 'Sophie Page',' - ','sophie@forwardrole.com')
	when 'ST' then concat(j.CreatingUser, ' - ', 'Steve Thompson',' - ','steve@forwardrole.com')
	when 'TOBY' then concat(j.CreatingUser, ' - ', 'Tom Byrne',' - ','tom@forwardrole.com')
	when 'TP' then concat(j.CreatingUser, ' - ', 'Thea Parry',' - ','thea@forwardrolerecruitment.com')
	when 'WIVE' then concat(j.CreatingUser, ' - ', 'Will Velios',' - ','will@forwardrolerecruitment.com')
	else j.CreatingUser end,'')
	--, coalesce('Creation date: ' + convert(varchar(10),j.CreationDate,120),'')
	, coalesce('Diary date: ' + convert(varchar(10),j.DiaryDate,120),'')
	--, coalesce('Diary entry type: ' + j.DiaryEntryType + ' - ' + ct3.Description,'')
	, coalesce('Subject: ' + j.Subject,'')
	) as Forward_comment_activities
, -10 as Forward_user_account_id
, case when j.DiaryDate is NULL then GETDATE()
	else j.DiaryDate end as Forward_insert_timestamp
, 'comment' as Forward_category
, 'contact' as Forward_type
from Journals j
left join CodeTables ct on ct.Code = j.CallType and ct.TabName = 'Call Types'
left join CodeTables ct2 on ct2.Code = j.ContactType and ct2.TabName = 'Jnl Contact Types'
left join CodeTables ct3 on ct3.Code = j.DiaryEntryType and ct3.TabName = 'Journal Entry Typ'
where j.ContactUniqueID > 0
order by j.ContactUniqueID

--total: 323794