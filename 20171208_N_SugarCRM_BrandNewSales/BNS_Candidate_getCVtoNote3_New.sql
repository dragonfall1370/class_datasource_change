CREATE TABLE CandidateCV_3rdNote (
candidateExternalId CHAR(50) PRIMARY KEY,
title char(36),
noteContent text
)
insert into CandidateCV_3rdNote
select
	concat('BNS_',tc.id) as 'candidateExternalId', 'CV' as title
	,ltrim(rtrim(cc.cv_c)) as 'noteContent'
from temp_candidates tc left join contacts_cstm cc on tc.id = cc.id_c
where cc.cv_c is not null and cc.cv_c <> ''
-- and tc.id = '1fa7cd4e-bf67-0602-24b0-48aed3c1cf50'