--Job application reference
select * from Match --145268

select * from Stage --343854

select * from Match_Stage
where Match_Stage_Id not in (-5,-4,-1,34)

---Check reference
select * from Match
where Match_Number = 52760

select s.Match_Number, s.Stage_Id, s.Match_Stage_Id, ms.Description, s.Created_DTTM, s.Created_By_Person_Number
from Stage s
left join Match_Stage ms on ms.Match_Stage_Id = s.Match_Stage_Id
where s.Match_Number = 52760

select * from Match where Match_Number = 135007

select * from Stage where Match_Number = 135007

select * from Job_Order

select Match_Number, count(*) from Placement
group by Match_Number
having count(*) > 1 --Match_Number 2246 has 2 entries

select * from Placement
where Match_Number = 2246

select * from Stage
where Match_Number = 2246

select * from Match_Stage

select * from Placement
where Permanent_Ind = 1

select * from AFR_JobCSV4

---
with maxInterview as (
select Match_Number, max(Interview_Sequence_Number) as maxInt
from Interview
group by Match_Number)

select distinct maxInt
from maxInterview

select * from Stage
where Match_Number = 72426

select Interview_DT, Interview_With from Interview