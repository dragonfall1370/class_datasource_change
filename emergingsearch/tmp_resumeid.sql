drop table if exists tmp_resumeid;
delimiter //

CREATE TABLE IF NOT EXISTS tmp_resumeid as (
        select CanID, concat(ID,'.',FileExtention) as name from canfiles
);

drop table if exists tmp_resumeid0;
CREATE TABLE IF NOT EXISTS tmp_resumeid0 as (
SELECT m.CanID, GROUP_CONCAT(distinct(g.name)) as name 
FROM canfiles m 
JOIN tmp_resumeid g ON m.CanID = g.Canid
GROUP BY m.CanID);

select * from tmp_resumeid0 where CanID = 81239700;
#select * from emergingsearch.canfiles;