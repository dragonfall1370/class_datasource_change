--CONTACT PRIMARY EMAIL
with SplitEmail as (select distinct ContactID
, value as SplitEmail
from EditedContact
cross apply string_split(EmailAddress,' ')

UNION ALL

select distinct ContactID
, value as SplitEmail
from EditedContactsInfo
cross apply string_split(Email,' '))

, EditedEmail as (select distinct ContactID
	, case when charindex('.',SplitEmail) = 1 then right(SplitEmail,case when len(SplitEmail) < 1 then 0 else len(SplitEmail)-1 end) --check if '.' begins
	when charindex('.',reverse(SplitEmail)) = 1 then left(SplitEmail,case when len(SplitEmail) < 1 then 0 else len(SplitEmail)-1 end) --check if '.' ends
	else SplitEmail end as EditedEmail
	from SplitEmail
	where SplitEmail like '%_@_%.__%')

, dup as (select ContactID
	, trim(' ' from translate(EditedEmail,'!'':"<>[]','        ')) as EmailAddress
	, row_number() over(partition by trim(' ' from translate(EditedEmail,'!'':"<>[]','        ')) order by ContactID asc) as rn
	from EditedEmail
	where EditedEmail like '%_@_%.__%')

--CONTACT EMAILS
, contactEmails as (select ContactID
	, string_agg(case when rn > 1 then concat (rn,'_',EmailAddress) else EmailAddress end,',') as contactEmails
	from dup
	group by ContactID)

--CONTACT OWNERS
, contactOwner as (select cc.ContactID, string_agg (u.Email,',') as contactOwner
	from CliContConsultants cc
	left join Users u on u.ConsultantID = cc.ConsultantID
	group by cc.ContactID)

--MAIN SCRIPT
select concat('TF',cc.ContactID) as 'contact-externalId'
, case when cc.ClientID in (select ClientID from ClientMaster) then concat('TF',cc.ClientID)
	else 'TF9999999' end as 'contact-companyId'
, case when c.FirstName = '' or c.FirstName is NULL then 'Firstname'
	else c.FirstName end as 'candidate-firstName'
, case when c.Surname = '' or c.Surname is NULL then 'Lastname'
	else c.Surname end as 'candidate-lastName'
, concat_ws(', '
	, nullif(concat_ws(' ',nullif(c.MobileArea,''),nullif(c.MobilePhone,'')),'')
	, nullif(concat_ws(' ',nullif(cc.PhoneArea,''),nullif(cc.WorkPhone,'')),'')
	, nullif(concat_ws(' ',nullif(c.HomeArea,''),coalesce(concat_ws(', ',nullif(c.HomePhone,''),nullif(c.HomePhone1,'')),'')),'')) as 'contact-phone'
, cc.PositionName as 'contact-jobTitle'
, ce.contactEmails as 'contact-email'
, cc.StreetState as contactCITY --CUSTOM SCRIPT
, concat_ws(char(10), concat('Contact external ID: ',cc.ContactID)
	, coalesce('OtherArea: ' + nullif(cc.OtherArea,''),'')
	, coalesce('OtherPhone: ' + nullif(cc.OtherPhone,''),'')
	, coalesce('Preferred Name: ' + nullif(c.PreferredName,''),'')
	, coalesce('Comments: ' + nullif(cc.Comments,''),'')
	, coalesce(nullif(c.Comments,''),'')
	) as 'contact-note'
from EditedContact cc
left join contactOwner co on co.ContactID = cc.ContactID
left join contactEmails ce on ce.ContactID = cc.ContactID
left join EditedContactsInfo c on c.ContactID = cc.ContactID
where cc.Status = 'A' --8433/ total: 16319

UNION ALL

select 'TF9999999','TF9999999','Contact','Default','','','','','This is default contact from data import'