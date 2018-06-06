
DROP TABLE IF EXISTS truong_company;


#CREATE TEMPORARY TABLE IF NOT EXISTS truong_company as
CREATE TABLE IF NOT EXISTS truong_company as
        SELECT 
            @row_number:=CASE
                        WHEN @customer_no = ltrim(rtrim(cname)) THEN @row_number + 1
                        ELSE 1
                        END AS num,
            @customer_no:= ltrim(rtrim(cname)) as cname, sno
        FROM staffoppr_cinfo
        ORDER BY ltrim(rtrim(cname)) desc;


select * from truong_company;
