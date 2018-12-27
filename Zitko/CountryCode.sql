select contact.id,MailingCountry, 
case when contact.MailingCountry = c.en_short_name then c.alpha_2_code
when contact.mailingcountry = 'England' then 'GB'
when contact.mailingcountry = 'FR' then 'FR'
when contact.mailingcountry = 'GB' then 'GB'
when contact.mailingcountry = 'Kingdom of Bahrain' then 'BH'
when contact.mailingcountry = 'Korea' then 'KR'
when contact.mailingcountry = 'Repubic of Korea (South)' then 'KR'
when contact.mailingcountry = 'Russia' then 'RU'
when contact.mailingcountry = 'Taiwan' then 'TW'
when contact.mailingcountry = 'The Netherlands' then 'NL'
when contact.mailingcountry = 'U.K.' then 'GB'
when contact.mailingcountry = 'U.S' then 'US'
when contact.mailingcountry = 'UK' then 'GB'
when contact.mailingcountry = 'United Kingdom' then 'GB'
when contact.mailingcountry = 'United Kingdom,' then 'GB'
when contact.mailingcountry = 'US' then 'US'
when contact.mailingcountry = 'USA' then 'US'
when contact.mailingcountry = 'Scotland' then ''
when contact.mailingcountry = 'KT1 4ER' then ''
else contact.mailingcountry end
as 'companycountry'

from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] b on b.Id = Contact.OwnerId
left join countries c on contact.MailingCountry = c.en_short_name
where RecordType.name='Contact'



