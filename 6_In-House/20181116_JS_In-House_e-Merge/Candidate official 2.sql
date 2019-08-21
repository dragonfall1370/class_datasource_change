with AGNote as ( select longtextcache.id, CndProfInfo.agencynotes, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.agencynotes = longtextcache.id),
IDJob as (select longtextcache.id, CndProfInfo.idealjob, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.idealjob = longtextcache.id),
sumary as (select longtextcache.id, CndProfInfo.summary, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.summary = longtextcache.id),
/*
CanFile as ( SELECT candid as 'CanID',
    STUFF((SELECT DISTINCT ', ' + filename
           FROM CndFiles a 
           WHERE a.candid = b.candid 
          FOR XML PATH('')), 1, 2, '') as 'Filename'
FROM CndFiles b
GROUP BY candid )
*/

textcv as (select longtextcache.id, CndProfInfo.textcv, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.textcv = longtextcache.id),

cademail as (select a.email, a.id, row_number() over (partition by a.email order by a.id) as 'emailnum' from VUser a),

consultant as (select CndPersInfo.id, email from CndPersInfo left join Vuser on CndPersInfo.consultantid = Vuser.id)

select
VUser.id as 'candidate-externalId',
iif(CndProfInfo.jobtitletxt is null or CndProfInfo.jobtitletxt='','',CndProfInfo.jobtitletxt) as 'candidate-jobTitle1',
iif(consultant.id = Vuser.id, iif(consultant.email is null,'',consultant.email),'') as 'company-owner',
VUser.firstname as 'candidate-firstName',
VUser.lastname as 'candidate-Lastname',
case when (cademail.emailnum = 1) then cademail.email
when (cademail.emailnum <> 1 and cademail.emailnum <> '') then concat('-dup',cademail.emailnum,'-',cademail.email)
when (cademail.email is null or cademail.email = '') then ''
else '' end as 'candidate-email',
iif(CndPersInfo.birthdate='' or CndPersInfo.birthdate is null,'',CndPersInfo.birthdate) as 'candidate-dob',
iif(CndPersInfo.addr1 is not null,concat(CndPersInfo.addr1,', ',CndPersInfo.Town,', ',CndPersInfo.zip,' ,',CndPersInfo.country),'') as 'candidate-address',
iif(CndPersInfo.Country='' or CndPersInfo.Country is null,'',CndPersInfo.Country) as 'candidate-Country',
iif(CndPersInfo.Town='' or CndPersInfo.Town is null,'',CndPersInfo.Town) as 'candidate-city',
iif(VUser.telephone='' or VUser.telephone is null,'',VUser.telephone) as 'candidate-primaryPhone',
iif(VUser.mobilephone='' or VUser.mobilephone is null,'',VUser.mobilephone) as 'candidate-mobile',
iif(CndPersInfo.zip = '' or CndPersInfo.zip is null,'',CndPersInfo.zip) as 'candidate-zipCode'



from VUser
left join CndPersInfo on VUser.id = CndPersInfo.id
left join CndProfInfo on VUser.id = CndProfInfo.id
left join location on CndProfInfo.curlocation = location.id
left join longtextcache on CndProfInfo.interviewnotes = longtextcache.id
left join CandRating on CndProfInfo.candrating = CandRating.ID
left join CandLevel on CndProfInfo.candlevel = CandLevel.id
left join AGNote on CndProfInfo.agencynotes = AGNote.id
left join IDJob on CndProfInfo.idealjob = IDJob.id
left join sumary on CndProfInfo.summary = sumary.id
left join textcv on CndProfInfo.textcv = textcv.id
left join cademail on VUser.id = cademail.id
left join consultant on VUser.id = consultant.id
