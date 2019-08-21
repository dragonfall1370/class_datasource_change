
with comment as (select contact.id, EmailSent.toEmail, contact.lastname, EmailSent.subject, EmailSent.messagebody, emailsent.timestamp,
row_number() over (partition by emailsent.subject, emailsent.timestamp order by contact.id) as 'row_num'
from contact left join EmailSent on contact.email = EmailSent.toEmail cross apply string_split(toEmail,';'))

select id, subject, messagebody, timestamp from comment where row_num = 1 and messagebody is not null 

