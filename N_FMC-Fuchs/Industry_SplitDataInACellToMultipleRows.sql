with
 mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(email,', ','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))

with t as
(SELECT 36 as ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT 36 as ID, CAST ('<M>' + REPLACE('FT ,MB, 123,asdflk, 8979ag',',','</M><M>') + '</M>' AS XML) AS Data) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
)
select ID, ltrim(rtrim(String)) from t

SELECT 36,'FT,MB'
	
