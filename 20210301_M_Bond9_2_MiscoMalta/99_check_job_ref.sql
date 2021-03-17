--Check job with conditions in main script
select uniqueid
, "2 company xref"
, "20 contact xref"
, *
from f03
where uniqueid = '80810301A3A48080' --com: 80810201F4838080 | con: 80810101B0B98080

select uniqueid
, "100 contact codegroup  23"
, "101 candidate codegroup  23"
from f01
where uniqueid = '80810101B0B98080'


select *
from f02
where uniqueid = '80810201AA8D8080'


--Check job app reference
select uniqueid
, "4 candidate xref"
, "6 job id xref"
, c94.description as last_stage
, c94_2.description as compl_stage
, c68.description as stage
from f13
left join (select * from codes where codegroup = '94') c94 on c94.code = "15 last actio codegroup  94"
left join (select * from codes where codegroup = '94') c94_2 on c94_2.code = "7 compl actn codegroup  94" --removed due to not accurate
left join (select * from codes where codegroup = '68') c68 on c68.code = "19 status codegroup  68"
where 1=1
and "4 candidate xref" in (select uniqueid from f01 where "101 candidate codegroup  23" = 'Y')
and "6 job id xref" in (select uniqueid from f03)
--total 85861


---JOB APP
select *
from f13
where "4 candidate xref" is not NULL
and "4 candidate xref" in (select uniqueid from f01)

--INVALID CANDIDATES
select *
from f13
where "4 candidate xref" is NULL --1967

--VALID PERSON | CANDIDATE
select *
from f13
where 1=1
--and "4 candidate xref" in (select uniqueid from f01) --86291
and "4 candidate xref" in (select uniqueid from f01 where "101 candidate codegroup  23" = 'Y') --86271 | valid candidates


--JOB APP REFERENCES
--
select *
from f13
where "4 candidate xref" = '80810101D2AA8180'
and "6 job id xref" = '80810301E7B28080'

--FROM PP3 > 
select *
from f01
where uniqueid = '80810101F1AC8280' --38588

--From ACT > FIELD 1
select *
from f01
where uniqueid = '8081010186F28180' --38588

select *
from f03
where uniqueid = '80810301D7B38080' --6773

select *
from act
where uniqueid = '8081FF01A5A69281'

select *
from act_pp3
where "pp3 uniqueid" = '8081FF01A5A69281' --80810101F1AC8280


--80810101D5E08180	80810301FAB08080	PLACED	2019-09-18	80810D01A89D8580	PERMANENT 
--80810101CCB98180	80810301E9A98080	PLACED	2017-03-10	80810D01BE8D8480	CONTRACT

select *
from act_pp3
where "28 contact xref" = '80810101D5E08180'

select *
from act
where uniqueid = '8081FF01D3FA8F81'

select *
from f03
where uniqueid = '80810301FAB08080'

---
select *
from act_pp3
where "28 contact xref" = '80810101CCB98180'

select *
from act
where uniqueid = '8081FF01D3FA8F81'

select *
from f03
where uniqueid = '80810301FAB08080'


--VALIDATE 80810101808D8180	80810301B9918080 8081FF01E5CFA280
select *
from act_pp3
where "pp3 uniqueid" = '8081FF01E5CFA280'


select *
from act_pp6
where "28 contact xref" = '80810101808D8180'