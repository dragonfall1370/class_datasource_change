with email as (select id, email from contact where contact.email like '%@%.%'),

dupmail as (select a.id, a.email, Row_number() over (partition by a.email
order by a.id asc) as 'emailrow' from contact a),

dupmail2 as (select a.id, a.email, a.emailrow from dupmail a where a.email like '%@%.%')

select contact.clientid as 'contact-companyId',
contact.id as 'contact-externalId',
contact.lastname as 'contact-lastName',
contact.firstname as 'contact-firstName',
case when (dupmail2.emailrow = 1) then dupmail2.email
when (dupmail2.email is null or dupmail2.email = '') 
then ''
else concat('dup',dupmail2.emailrow,'-',dupmail2.email) end as 'contact-email',
contact.telephone as 'contact-phone',
contact.position as 'contact-jobTitle',
concat('Active Flag: ', (iif(contact.activeflag = 1,'Active','InActive')),',',(char(13)+char(10)),
nullif(concat('Fax Number: ',contact.faxnumber),'Fax Number: '),(char(13)+char(10)),
nullif(concat('Comments: ',longtextcache.chunk),'Comments: ')) as 'contact-Note'

from contact
left join email on contact.id = email.id
left join longtextcache on contact.comments = longtextcache.id
left join dupmail2 on contact.id = dupmail2.id