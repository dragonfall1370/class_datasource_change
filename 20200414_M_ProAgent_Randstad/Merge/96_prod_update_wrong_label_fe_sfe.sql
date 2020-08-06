---UPDATE LABEL FROM DB BACKUP | RANDSTAD MAPPING
select *
from vc_2_vc_new_fe_sfe
where vc_fe_id = 2998 and vc_sfe_id = 247

--
update vc_2_vc_new_fe_sfe
set vc_new_fe = 'Web Design
ウェブデザイン'
, vc_new_fe_en = 'Web Design'
, vc_new_fe_ja = 'ウェブデザイン'
where vc_fe_id = 2998 and vc_sfe_id = 247


---UDPATE LABEL FROM VINCERE
select *
from sub_functional_expertise
where functional_expertise_id = 3044

update sub_functional_expertise
set name = 'Financial Planning & Analysis (Manager) / ファイナンシャルプランニング & アナリシス (マネージャー)'
where id = 652
and functional_expertise_id = 3044;

update sub_functional_expertise
set name = 'Financial Planning & Analysis (Staff) / ファイナンシャルプランニング & アナリシス (スタッフ)'
where id = 653
and functional_expertise_id = 3044;


--
select *
from sub_functional_expertise
where functional_expertise_id = 3050

update sub_functional_expertise
set name = 'In-house Counsel, Of Counsel / インハウスカウンセル（社内弁護士）, オブカウンセル'
where id = 747
and functional_expertise_id = 3050;