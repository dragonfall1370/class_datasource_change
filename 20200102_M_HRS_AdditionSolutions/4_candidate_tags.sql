--CANDIDATE TAGS CF
with tags as (select m._fk_contact, m._fk_tag , t.name as tag_name
	from [20191030_155622_mapping] m
	left join [20191030_153350_contacts] c on c.__pk = m._fk_contact
	left join [20191030_160039_tags] t on t.__pk = m._fk_tag
	where m._fk_contact > 0
	and m._fk_tag <> '?' --special characters
	and c.type = 'Candidate'
	and m._fk_contact > 79567)

/* AUDIT TAG NAME
select distinct tag_name from tags
*/

select concat('AS', _fk_contact) as cand_ext_id
, string_agg(tag_name, ', ') within group (order by _fk_tag asc) as tag_name
from tags
group by _fk_contact