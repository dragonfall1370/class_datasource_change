--Candidate source
select [PANO ] as cand_ext_id
, 登録経路 as original_source
, case 登録経路
	when 'ホームページ' then 'Web Site'
	when 'ホームページ（チャレンジド）' then 'Web Site'
	when 'ミドルの転職' then 'en Middle'
	when 'enミドル（成功報酬）' then 'en Middle (Success fee)'
	when 'AMBI（成功報酬）' then 'AMBI (Success fee)'
	when 'en転職 (成功報酬)' then 'en Tenshoku (Success fee)'
	when '日経WOMANキャリア（成功報酬）' then 'Nikkei WOMAN (Success fee)'
	when '日経キャリアNET' then 'Nikkei Career NET'
	when 'ダイジョブ.com' then 'Daijob.com'
	when 'Gaijin Pot' then 'Gaijin Pot'
	when 'リクナビNEXT' then 'Rikunabi Next'
	when 'CAREER CARVER（成功報酬）' then 'Career Carver'
	when 'DODA(成功報酬）' then 'DODA (Success fee)'
	when 'iX Professional Search（成功報酬）' then 'iX Professional Search (Success fee)'
	when 'ビズリーチ(成功報酬)' then 'BizReach'
	when 'キャリトレ（成功報酬）' then 'Careertrek (Success fee)'
	when '日経TECHキャリア（成功報酬）' then 'Nikkei TECH Career (Success fee)'
	when 'イーキャリアＦＡ' then 'eCareer FA'
	when 'LinkedIn' then 'LinkedIn'
	when 'マイナビ(成功報酬）' then 'Mynavi (Success fee)'
	when 'マイナビ転職エージェント' then 'Mynavi'
	when '日経WOMANキャリア' then 'Nikkei WOMAN (Success fee)'
	when 'キャリコネ' then 'Other'
	when '転職のかんづめ' then 'Tenshoku no Kanzume'
	when 'CodeIQ（成功報酬）' then 'Code IQ (Success fee)'
	when '紹介会社(成功報酬)' then 'HR Angecy (Success fee)'
	when '転職会議(成功報酬)' then 'Tenshoku Kaigi (Success fee)'
	when 'LiBz CAREER(成功報酬)' then 'LiBz CAREER (Success fee)'
	when 'F21 登録スタッフ' then 'F21 staff'
	when '【新卒】ホームページ' then 'Web Site'
	else 'Other' end candidate_source
from csv_can
where nullif(登録経路, '') is not NULL