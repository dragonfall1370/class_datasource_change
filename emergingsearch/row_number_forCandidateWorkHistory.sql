#CREATE TEMPORARY TABLE IF NOT EXISTS estmp as (
#DROP TABLE tmp;
CREATE TABLE IF NOT EXISTS tmp as (
SELECT 
    @row_number:=CASE
        WHEN @customer_no = canid THEN @row_number + 1
        ELSE 1
    END AS num,
    @customer_no:=canid as CanID,
    StartDate,
    EndDate,
    Employer,
    JobTitle,
    Note
FROM canemployer
#WHERE StartDate is not null;
ORDER BY CanId, StartDate
#ORDER BY CanID;
);
