 
select sno, ltrim(rtrim(replace(email,CHAR(9),'') )) from staffoppr_contact where sno in (1829,1835,6684,5167)


DROP TABLE IF EXISTS truong_contact;
#CREATE TEMPORARY TABLE IF NOT EXISTS truong_contact as
CREATE TABLE IF NOT EXISTS truong_contact as
        SELECT 
            @row_number:=CASE
                        WHEN @customer_no = ltrim(rtrim(replace(email,CHAR(9),'') )) THEN @row_number + 1
                        ELSE 1
                        END AS num,
            @customer_no:= ltrim(rtrim(replace(email,CHAR(9),'') )) as email, sno
        FROM staffoppr_contact where ltrim(rtrim(replace(email,CHAR(9),'') )) <> ''
        ORDER BY ltrim(rtrim(replace(email,CHAR(9),'') )) desc;


select * from truong_contact;
