
DROP TABLE IF EXISTS truong_job;
CREATE TABLE IF NOT EXISTS truong_job as      
       SELECT
                         @postitle_rank := IF(@postitle = case postitle when '' then 'No JobTitle' else postitle end and contact = @contact, @postitle_rank + 1, 1) AS postitle_rank,
                         @contact := contact as contact,
                         @postitle := case postitle when '' then 'No JobTitle' else postitle end as postitle,
                         posted_date,
                         posid
       FROM posdesc
       where contact in (0,4)
       ORDER BY contact,postitle DESC;

select * from truong_job where contact = 0;
select count(*) from truong_job;