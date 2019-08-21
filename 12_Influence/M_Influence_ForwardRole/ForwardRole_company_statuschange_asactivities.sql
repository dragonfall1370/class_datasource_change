select AmendmentDate
, CreatingUser
, CreationDate
, NewValue
, ObjectType
, OldValue
, Type
, UniqueID
, ObjectUniq as Forward_CompExtID
, concat_ws (char(10)
	, coalesce('Amendment Date: ' + convert(varchar(10),AmendmentDate,120),'')
	, coalesce('Creating user: ' + case CreatingUser
	when 'ADM' then concat(CreatingUser, ' - ', 'Admin',' - ','No email adress - admin login')
	when 'ADMT' then concat(CreatingUser, ' - ', 'Staff Training Login')
	when 'AH' then concat(CreatingUser, ' - ', 'Alison Hart',' - ','alison@forwardrolerecruitment.com')
	when 'AM' then concat(CreatingUser, ' - ', 'Adam Miller',' - ','adam@forwardrolerecruitment.com')
	when 'ARAL' then concat(CreatingUser, ' - ', 'Arif Ali',' - ','arif@forwardrole.com')
	when 'BJ' then concat(CreatingUser, ' - ', 'Brian Johnson',' - ','brian@forwardrole.com')
	when 'BRTO' then concat(CreatingUser, ' - ', 'Brad Thomas',' - ','brad@forwardrole.com')
	when 'BS' then concat(CreatingUser, ' - ', 'Becky Smith',' - ','becky@forwardrole.com')
	when 'CP' then concat(CreatingUser, ' - ', 'Camilla Purdy',' - ','camilla@forwardrole.com (ADMIN) ')
	when 'CT' then concat(CreatingUser, ' - ', 'Chris Thomason',' - ','chris@forwardrolerecruitment.com')
	when 'DAHA' then concat(CreatingUser, ' - ', 'Danielle Harvey',' - ','danielle@forwardrolerecruitment.com')
	when 'DAHY' then concat(CreatingUser, ' - ', 'Dan Haydon',' - ','danh@forwardrole.com')
	when 'DANO' then concat(CreatingUser, ' - ', 'David Nottage',' - ','david@forwardrolerecruitment.com')
	when 'DETO' then concat(CreatingUser, ' - ', 'Desislava Torodova',' - ','desi@forwardrole.com')
	when 'DM' then concat(CreatingUser, ' - ', 'Dan Middlebrook',' - ','dan@forwardrole.com')
	when 'DS' then concat(CreatingUser, ' - ', 'Dominic Scales',' - ','dominic@forwardrolerecruitment.com')
	when 'EMME' then concat(CreatingUser, ' - ', 'Emma Melling',' - ','emma@forwardrole.com')
	when 'GRBO' then concat(CreatingUser, ' - ', 'Grant Bodie',' - ','grant@forwardrole.com')
	when 'GW' then concat(CreatingUser, ' - ', 'Guy Walker',' - ','guy@forwardrole.com')
	when 'HB' then concat(CreatingUser, ' - ', 'Henna Baig',' - ','henna@forwardrolerecruitment.com')
	when 'HC' then concat(CreatingUser, ' - ', 'Helen Colley',' - ','helen@forwardrole.com')
	when 'IL' then concat(CreatingUser, ' - ', 'Ian Lenahan',' - ','ian@forwardrolerecruitment.com')
	when 'IZKH' then concat(CreatingUser, ' - ', 'Izzy Khan',' - ','izzy@forwardrole.com')
	when 'JH' then concat(CreatingUser, ' - ', 'Jack Harrison',' - ','jack@forwardrolerecruitment.com')
	when 'JOPE' then concat(CreatingUser, ' - ', 'Josh Pepper',' - ','josh@forwardrole.com')
	when 'JS' then concat(CreatingUser, ' - ', 'Jon Saxon',' - ','jon@forwardrolerecruitment.com')
	when 'KM' then concat(CreatingUser, ' - ', 'Katrina McCafferty',' - ','katrina@forwardrolerecruitment.com')
	when 'LK' then concat(CreatingUser, ' - ', 'Lucy Ketley',' - ','lucy@forwardrolerecruitment.com')
	when 'MABO' then concat(CreatingUser, ' - ', 'Matthew Borthwick',' - ','mattb@forwardrole.com')
	when 'MD' then concat(CreatingUser, ' - ', 'Matt Darwell',' - ','matt@forwardrole.com')
	when 'MIRH' then concat(CreatingUser, ' - ', 'Mike Rhodes',' - ','mike@forwardrole.com')
	when 'NAYO' then concat(CreatingUser, ' - ', 'Nathan Young',' - ','nathan@forwardrole.com')
	when 'PAMC' then concat(CreatingUser, ' - ', 'Patrick McMahon',' - ','patrick@forwardrole.com')
	when 'PAWE' then concat(CreatingUser, ' - ', 'Paddy Wells',' - ','paddy@forwardrole.com')
	when 'PHST' then concat(CreatingUser, ' - ', 'Phill Stott',' - ','phill@forwardrole.com')
	when 'RADA' then concat(CreatingUser, ' - ', 'Rachel Davies',' - ','rachel@forwardrole.com')
	when 'RAWH' then concat(CreatingUser, ' - ', 'Rachel Wheeler',' - ','rachelw@forwardrole.com')
	when 'RF' then concat(CreatingUser, ' - ', 'Ricardo Facchin')
	when 'RYDO' then concat(CreatingUser, ' - ', 'Ryan Dolan',' - ','ryan@forwardrole.com')
	when 'SASH' then concat(CreatingUser, ' - ', 'Sam Shinners',' - ','sam@forwardrolere.com')
	when 'SOPA' then concat(CreatingUser, ' - ', 'Sophie Page',' - ','sophie@forwardrole.com')
	when 'ST' then concat(CreatingUser, ' - ', 'Steve Thompson',' - ','steve@forwardrole.com')
	when 'TOBY' then concat(CreatingUser, ' - ', 'Tom Byrne',' - ','tom@forwardrole.com')
	when 'TP' then concat(CreatingUser, ' - ', 'Thea Parry',' - ','thea@forwardrolerecruitment.com')
	when 'WIVE' then concat(CreatingUser, ' - ', 'Will Velios',' - ','will@forwardrolerecruitment.com')
	else CreatingUser end,'')
	, coalesce('Creation Date: ' + convert(varchar(10),CreationDate,120),'')
	, coalesce('Subject: ' + nullif(NewValue,''),'')
	, coalesce('Object Type: ' + nullif(ObjectType,''),'')
	, coalesce('Object Uniq: ' + nullif(convert(varchar(max),ObjectUniq),''),'')
	, coalesce('Old Value: ' + nullif(OldValue,''),'')
	, coalesce('Type: ' + nullif(Type,''),'')
	, coalesce('Unique ID: ' + nullif(convert(varchar(max),UniqueID),''),'')
	) as Forward_comment_activities
, -10 as Forward_user_account_id
, 'comment' as Forward_category
, 'company' as Forward_type
, case when CreationDate is not NULL then CreationDate
	else getdate() end as Forward_insert_timestamp
from StatusChange
where ObjectType = 'SITE'
and ObjectUniq in (select SiteUniqueID from Sites) --4649