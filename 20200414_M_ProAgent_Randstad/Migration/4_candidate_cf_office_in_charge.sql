/* Newly added office in charge --20200211

insert into LP_Office (pa_office, vc_office_jap, vc_office_en, category) values ('', '藤枝事業所', 'Fujieda Office', 'newly added');
insert into LP_Office (pa_office, vc_office_jap, vc_office_en, category) values ('', '五泉事業所', 'Gosen Office', 'newly added');
insert into LP_Office (pa_office, vc_office_jap, vc_office_en, category) values ('', '日立事業所', 'Hitachi office', 'newly added');
insert into LP_Office (pa_office, vc_office_jap, vc_office_en, category) values ('', '新宿区キャリアセンター', 'Shinjuku Career Center', 'newly added');
insert into LP_Office (pa_office, vc_office_jap, vc_office_en, category) values ('', '宇都宮北事業所', 'Utsunomiya Kita Office', 'newly added');
*/


--#CF | Office in charge | Multiple selection (1 value at current)
select c.[PANO ] as cand_ext_id
, c.人材担当
, case when c.人材担当 not in (select pa_office from LP_Office where category = 'candidate') then 'Impossible to distribute'
	when c.人材担当 in (select pa_office from LP_Office where category = 'candidate') then l.vc_office_en
	else NULL end as field_value --job_office
, 'add_cand_info' as additional_type
, 1139 as form_id
, 11312 as field_id
, current_timestamp as insert_timestamp
from csv_can c
left join (select * from LP_Office where category = 'candidate') l on l.pa_office = c.人材担当
where nullif(c.人材担当, '') is not NULL