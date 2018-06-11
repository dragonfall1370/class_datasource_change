CREATE TABLE temp_can_CV (
candidateExternalId CHAR(50) PRIMARY KEY,
userId int,
CommentTimestamp datetime,
InsertTimeStamp datetime,
AssignedUserId int,
RelatedStatus int,
CommentContent text
)

insert into temp_can_CV
select
concat('BNS_',tc.id) as CandidateExternalId, -10 as userId
		, coalesce(tc.date_entered,now()) as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type-- ,1 as RelatedStatus
		,concat('CV TEXT:',char(10),ltrim(rtrim(cc.cv_c))) as 'CommentContent'
from temp_candidates tc left join contacts_cstm cc on tc.id = cc.id_c
where cc.cv_c is not null and cc.cv_c <> ''
-- and tc.id = '1fa7cd4e-bf67-0602-24b0-48aed3c1cf50'