
DROP TABLE IF EXISTS tmp;


#CREATE TEMPORARY TABLE IF NOT EXISTS tmp as
CREATE TABLE IF NOT EXISTS tmp as
        SELECT 
            @row_number:=CASE
                        WHEN @customer_no = canid THEN @row_number + 1
                        ELSE 1
                        END AS num,
            @customer_no:=canid as CanID,
            IF(ISNULL(StartDate),"",StartDate) as StartDate,
            EndDate,
            Employer,
            JobTitle,
            Note
        FROM canemployer
        ORDER BY CanId,StartDate desc;


select * from tmp;