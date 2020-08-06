--Update candidates owners for updated user mapping
select --top 1000 
	c.[PANO ] as cand_ext_id
	, trim(u.EmailAddress) as owner_email
	, case 
	when [人材担当ユーザID] = 'FPC51' then '[{"ownerId":29707,"primary":true}]'  --Tetsuya.Uenuma@randstad.co.jp
	when [人材担当ユーザID] = 'FPC63' then '[{"ownerId":29030,"primary":true}]'  --candidate.div@randstad.co.jp
	when [人材担当ユーザID] = 'FPC70' then '[{"ownerId":28990,"primary":true}]'  --kasumi.konishi@randstad.co.jp
	end as candidate_owner
from csv_can c
left join UserMapping u on u.UserID = c.[人材担当ユーザID]
where c.[人材担当ユーザID] in ('FPC51', 'FPC63', 'FPC70')
and c.[チェック項目] not like '%チャレンジド人材%' --171801