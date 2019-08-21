create table importJob
(positioncontactId nvarchar(20),
CompanyID int,
MainContactId int,
ContactID int,
positionexternalId nvarchar(20),
positiontitleold nvarchar(500),
positiontitle nvarchar(500),
positionheadcount int,
positioncurrency nvarchar(20),
positiontype nvarchar(100),
positionowners nvarchar(500),
positioninternalDescription nvarchar(max),
positioncomment nvarchar(max),
positionstartDate date,
positiondocument nvarchar(max),
positionendDate date,
positionnote nvarchar(max)
)