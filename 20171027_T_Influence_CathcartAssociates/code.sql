
select Code, Description,TabName from CodeTables where Code in ('BUSD','CBN','CDQU','CLEM','CLF','CLII','CLTK','CNDI','CNFM','CNTR','FAIL','IF','JOB','MCAN','MCON','MEET','OFF','OSCV') and TabName = 'Call Types'

--
select distinct c.BusinessType001
        , c.UniqueID as 'company-externalId'
        , case c.BusinessType001
                when 'CALL' then 28886
                when 'CONST' then 28888
                when 'CONSU' then 28889
                when 'DIGIT' then 28890
                when 'ECOM' then 28891
                when 'EDU' then 28892
                when 'FIN' then 28893
                when 'FMCG' then 28813
                when 'LOG' then 28895
                when 'MANU' then 28896
                when 'NHS' then 28897
                when 'O&G' then 28898
                when 'PHARM' then 28899
                when 'PUBLI' then 28900
                when 'RETL' then 28780
                when 'SOFT' then 28902
                when '3RD' then 28903
                when 'UTIL' then 28778
                else '' end as INDUSTRY
        , co.description
        , CURRENT_TIMESTAMP
from clients c
left join CodeTables co on co.Code = c.BusinessType001
where co.Code <> '' and co.Code is not null and TabName = 'Bus Type' and c.UniqueID in ('462','1161')

-- CANDIDATE
select distinct
select DISTINCT 
        c.status
        , ct.description
from candidates c
left join (select code,description from codetables where TabName = 'Candidate Status') ct on ct.code = c.status



-- VACANCIES
select 
        DISTINCT v.status
        , ct.description
from vacancies v
left join (select code,description from codetables where TabName = 'Vac Status Code') ct on ct.code = v.status
