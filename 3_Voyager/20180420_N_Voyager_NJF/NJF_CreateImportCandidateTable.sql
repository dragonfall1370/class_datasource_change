create table ImportCandidate
(
candidateexternalId nvarchar(20),
PersonId int,
candidatefirstName nvarchar(100),
candidateLastname nvarchar(100),
candidateMiddlename nvarchar(100),
candidateemail nvarchar(200),
candidateworkEmail nvarchar(200),
candidatedob date,
candidatetitle nvarchar(10),
candidategender	nvarchar(10),
candidatephone nvarchar(100),
candidatemobile	nvarchar(100),
candidateworkPhone nvarchar(100),
candidatehomephone nvarchar(100),
candidatelinkedin nvarchar(1000),
candidateskype nvarchar(500),
candidateaddress nvarchar(500),
candidatecity nvarchar(100),
candidatestate nvarchar(100),
candidatezipCode nvarchar(100),
candidateCountry nvarchar(100),
candidateowners nvarchar(500),
candidatecitizenship nvarchar(100),	
candidatejobTitle1 nvarchar(500),
candidateemployer1 nvarchar(500),
candidateworkHistory nvarchar(max),
candidateresume nvarchar(max),
candidateskills	nvarchar(max),
candidatenote nvarchar(max)
)

select count(*) from ImportCandidate
select distinct candidateexternalId from ImportCandidate