with test as (select
code as 'contact-externalId',
clientcode as 'contact-companyId',
upper(replace(title,'.','')) as 'contact-title',
iif(surname is null or surname ='','No Last Name',surname) as 'contact-lastName',
iif(forename is null or forename = '','No First Name',forename) as 'contact-firstName',
middlename as 'contact-middleName',
salutation as 'preferred-name',
tel as 'contact-phone',
email as 'contact-email',
a.position as 'contact-jobtitle',
dept as 'contact-department',
a.mobile as 'contact-mobilephone',
url as 'contact-linkedin',
b.username as 'contact-owners',
location as 'contact-location',
emailadd1 as 'email1',
concat('External ID: ',code,(char(13)+char(10)),
nullif(concat('Note: ',notes),'Note: ')) as 'contact-notes'
from contact a
left join secure b on a.owningcons = b.initials)

,test2 as (select ROW_NUMBER() over (partition by [contact-email] order by [contact-email]) as rn,* from test)

select iif([contact-email] = '' or [contact-email] is null,'',iif(rn=1,[contact-email],concat(rn,'-',[contact-email]))) as email,* from test2





