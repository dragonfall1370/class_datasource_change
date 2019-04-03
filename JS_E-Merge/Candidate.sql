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

cademail as (select a.email, a.id, row_number() over (partition by a.email order by a.id) as 'emailnum' from VUser a)

select
VUser.id as 'candidate-externalId',
iif(CndProfInfo.jobtitletxt is null or CndProfInfo.jobtitletxt='','',CndProfInfo.jobtitletxt) as 'candidate-title',
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
iif(VUser.telephone='' or VUser.telephone is null,'',VUser.telephone) as 'candidate-homePhone',
iif(VUser.mobilephone='' or VUser.mobilephone is null,'',VUser.mobilephone) as 'candidate-mobile',
concat((iif(CndPersInfo.IDNumber is null or CndPersInfo.IDNumber='','',concat('ID Number: ',CndPersInfo.IDNumber))), (char(13)+char(10)),
(iif(CndProfInfo.curlocation = location.id,concat('Current Location: ',location.description),'')),(char(13)+char(10)),
(iif(CndProfInfo.location = location.id,concat('Prefered Location: ',location.description),'')),
(iif(CndProfInfo.id = VUser.id,concat('Salary: ',CndProfInfo.Salary),'')),(char(13)+char(10)),
iif(CndProfInfo.id = VUser.id,concat('Reference Code: ',CndProfInfo.refcodes),''),(char(13)+char(10)),
iif(CndProfInfo.Candlevel = Candlevel.id and CndProfInfo.candlevel is not null,concat('Candidate Level: ', CandLevel.description),''),(char(13)+char(10)),
iif(CndProfInfo.CandRating = CandRating.id and CndProfInfo.candrating is not null,concat('Candidate Rating: ', CandRating.description),''),(char(13)+char(10)),
iif(CndProfInfo.Interviewed='1','Interviewed Status: Yes', 'Interviewed Status: No'),(char(13)+char(10)),
iif(CndProfInfo.agencynotes = AGNote.ID, AGnote.chunk,''),(char(13)+char(10)),
nullif(concat('Ideal Job: ',IDJob.chunk),'Ideal Job: '),(char(13)+char(10)),
iif(CndProfInfo.summary is null or CndProfInfo.summary = '','',concat('Summary: ',longtextcache.chunk)),iif(CndProfInfo.interviewnotes is null or CndProfInfo.interviewnotes='','',concat('Interview Notes: ',longtextcache.chunk)),(char(13)+char(10)),(char(13)+char(10)),
nullif(concat('CV: ',textcv.chunk),'CV: '),(char(13)+char(10)),
iif(CndProfInfo.refcodesmax is null or CndProfInfo.refcodesmax='','',concat('Ref Code: ',CndProfInfo.refcodesmax))
) as 'candidate-note',
iif(CndPersInfo.zip = '' or CndPersInfo.zip is null,'',CndPersInfo.zip) as 'candidate-zipCode',
iif(CndPersInfo.consultantid is null or CndPersInfo.consultantid = '','',CndPersInfo.consultantid) as 'candidate-owners'


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

where VUser.id between 110000 and 130000