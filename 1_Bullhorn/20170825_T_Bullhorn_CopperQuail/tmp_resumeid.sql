/*
select count(*) from canfiles where FileExtention is not null and FileExtention not like '% %'; #478
select count(*) from CVblob; #106
select count(*) from fCVblob; #5
select count(*) from PhotoBlob; #88
select * from canfiles where FileExtention is not null and FileExtention not like '% %' limit 10;
select * from CVblob limit 10;
select * from fCVblob limit 10;
select * from PhotoBlob limit 10;
*/

drop table if exists tmp_document;
CREATE TABLE IF NOT EXISTS tmp_document as
        (
        SELECT m.CanID, GROUP_CONCAT(distinct(m.name)) as name 
        FROM    (
                select CanID, concat(ID,'.',FileName) as name from canfiles where FileExtention is not null and FileExtention not like '% %'
                union all
                select CanID, concat(ID,'.',FileName) as name from CVblob
                union all
                select CanID, concat(ID,'.',FileName) as name from fCVblob
                #select CanID, concat(ID,'.',FileName) as name from PhotoBlob >>> candidate photo
                ) m 
        GROUP BY m.CanID
        );

select * from tmp_document where CanID = 81239700;

/*
drop table if exists tmp_resumeid;
CREATE TABLE IF NOT EXISTS tmp_resumeid as
        (
        select CanID, concat(ID,'.',FileName) as name from canfiles union all
        (select CanID, concat(ID,'.',FileName) as name from CVblob) union all
        (select CanID, concat(ID,'.',FileName) as name from fCVblob) union all
        (select CanID, concat(ID,'.',FileName) as name from PhotoBlob)
        );
select * from tmp_resumeid;      
*/