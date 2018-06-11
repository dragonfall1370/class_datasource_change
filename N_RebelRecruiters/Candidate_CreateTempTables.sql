create table tempcandlocation (
cand_ref nvarchar(max), 
iso_2 nvarchar(max), 
locationName nvarchar(max)
)

insert into tempcandlocation 
select cand_ref, iso_2, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(cand_address, ''), '')
			+ Coalesce(', ' + NULLIF(cand_address2, ''), '')
			+ Coalesce(', ' + NULLIF(cand_town, ''), '')
			+ Coalesce(', ' + NULLIF(cand_county, ''), '')
			+ Coalesce(', ' + NULLIF(cand_pcode, ''), '')
			+ Coalesce(', ' + NULLIF(country_name, ''), '')
			, 1, 1, '')) as 'locationName'
	from candidate can left join country c on can.cand_country = c.country_ref
----------------------------------------

create table additionalLoc (
cand_ref nvarchar(max), 
loc2 nvarchar(max), 
)

with loc2 as (
select c.cand_ref, l.description--, cl.link_ref, cl.loc_ref
from candidate c left join candlocation cl on c.cand_ref = cl.cand_ref
left join location l on cl.loc_ref = l.loc_ref
where cl.loc_ref <> 0)

insert into additionalLoc 
SELECT cand_ref,
     STUFF(
         (SELECT ', ' + description
          from  loc2
          WHERE cand_ref = l.cand_ref
    order by cand_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS loc2
FROM loc2 as l
GROUP BY l.cand_ref

----------------------------------------------Get documents
create table candocuments (
cand_ref nvarchar(max), 
canddocs nvarchar(max), 
)

with temp as (select cand_ref,  coalesce(cand_ref + '_' + doc_ref + '.' + doc_ext,'') as doc
from canddocs 
where left(cand_ref,1) in ('1','2','3','4','5','6','7','8','9','0')
	and left(doc_ref,1) in ('1','2','3','4','5','6','7','8','9','0'))

insert into candocuments
SELECT cand_ref,
     STUFF(
         (SELECT ',' + doc
          from  temp
          WHERE cand_ref = cd.cand_ref
    order by cand_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS canddocs
FROM temp as cd
GROUP BY cd.cand_ref

-----------------------------------------Job Title 1
create table cand_jobtitle (
cand_ref nvarchar(max), 
canjobtitle nvarchar(max), 
)
with temp_role as (
select cr.link_ref, cr.cand_ref, r.description
from candrole cr left join role r on cr.role_ref = r.role_ref)

insert into cand_jobtitle 
SELECT cand_ref, 
     STUFF(
         (SELECT ', ' + description
          from  temp_role
          WHERE cand_ref =tr.cand_ref
    order by cand_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,2,'')  AS canjobtitle
FROM temp_role as tr
GROUP BY tr.cand_ref

-----------------------Skill
create table candidateskill (
cand_ref nvarchar(max), 
skill nvarchar(max), 
)

with skill1 as (
select c.cand_ref, s.description
from candidate c left join candskill cs on c.cand_ref = cs.cand_ref
left join skill s on cs.skill_ref = s.skill_ref
where cs.skill_ref <> 0)

insert into candidateskill 
SELECT cand_ref,
     STUFF(
         (SELECT ', ' + description
          from  skill1
          WHERE cand_ref = s.cand_ref
    order by cand_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS skill
FROM skill1 as s
GROUP BY s.cand_ref