---Status change as comment activties
select distinct ObjectType from StatusChange

/*
---
BKHD
CAND
COMP
CONT
IVH
MAT
SITE
VAC
---
*/

select * from StatusChange
where ObjectType = 'SITE' --5487

select * from StatusChange
where ObjectType = 'COMP' --8198

select * from StatusChange
where ObjectType = 'CAND' --60780

select * from StatusChange
where ObjectType = 'CONT' --29561

select * from StatusChange
where ObjectType = 'VAC' --13005