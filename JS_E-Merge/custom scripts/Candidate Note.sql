with AGNote as ( select longtextcache.id, CndProfInfo.agencynotes, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.agencynotes = longtextcache.id),
IDJob as (select longtextcache.id, CndProfInfo.idealjob, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.idealjob = longtextcache.id),
sumary as (select longtextcache.id, CndProfInfo.summary, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.summary = longtextcache.id),


textcv as (select longtextcache.id, CndProfInfo.textcv, longtextcache.chunk from CndProfInfo left join longtextcache on CndProfInfo.textcv = longtextcache.id),

cademail as (select a.email, a.id, row_number() over (partition by a.email order by a.id) as 'emailnum' from VUser a),

summary as (select CndProfInfo.id, chunk from CndProfInfo left join longtextcache on CndProfInfo.summary = longtextcache.id)

select
VUser.id as 'candidate-externalId',
concat(concat('EXTERNAL ID: ',VUser.id,(char(13)+char(10))),
(iif(CndPersInfo.IDNumber is null or CndPersInfo.IDNumber='','',concat('ID NUMBER: ',CndPersInfo.IDNumber))), (char(13)+char(10)),(char(13)+char(10)),
(iif(CndProfInfo.curlocation = location.id,concat('CURRENT LOCATION: ',location.description),'')),(char(13)+char(10)),(char(13)+char(10)),
(iif(CndProfInfo.location = location.id,concat('PREFERED LOCATION: ',location.description),'')),(char(13)+char(10)),(char(13)+char(10)),
(iif(CndProfInfo.id = VUser.id,concat('SALARY: ',CndProfInfo.Salary),'')),(char(13)+char(10)),(char(13)+char(10)),
iif(CndProfInfo.id = VUser.id,nullif(concat('REFERENCE CODE: ',CndProfInfo.refcodes,(char(13)+char(10)),(char(13)+char(10))),concat('REFERENCE CODE: ',(char(13)+char(10)),(char(13)+char(10)))),''),
iif(CndProfInfo.Candlevel = Candlevel.id and CndProfInfo.candlevel is not null,concat('CANDIDATE LEVEL: ', CandLevel.description,(char(13)+char(10)),(char(13)+char(10))),''),
iif(CndProfInfo.CandRating = CandRating.id and CndProfInfo.candrating is not null,concat('CANDIDATE RATING: ', CandRating.description,(char(13)+char(10)),(char(13)+char(10))),''),
iif(CndProfInfo.Interviewed='1','INTERVIEWED STATUS: Yes', 'INTERVIEWED STATUS: NO'),(char(13)+char(10)),(char(13)+char(10)),
iif(CndProfInfo.summary is null or CndProfInfo.summary = '','',concat('SUMMARY: ',(char(13)+char(10)),summary.chunk)),iif(CndProfInfo.interviewnotes is null or CndProfInfo.interviewnotes='','',concat('INTERVIEW NOTE: ',longtextcache.chunk,(char(13)+char(10)),(char(13)+char(10)))),
iif(CndProfInfo.agencynotes = AGNote.ID, concat('AGENCY NOTE:',(char(13)+char(10)), AGnote.chunk),''),(char(13)+char(10)),
nullif(concat('IDEAL JOB: ',IDJob.chunk),'IDEAL JOB: '),(char(13)+char(10)),(char(13)+char(10)),
nullif(concat('CV: ',textcv.chunk),'CV: '),(char(13)+char(10)),(char(13)+char(10)),
iif(CndProfInfo.refcodesmax is null or CndProfInfo.refcodesmax='','',concat('REF CODE: ',(char(13)+char(10)),CndProfInfo.refcodesmax))
) as 'candidate-note'



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
left join summary on Vuser.id = summary.id
