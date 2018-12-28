create table email
(AttachmentID int,
contactID int,
companyID int,
sentdate varchar(60),
receiveddate varchar(60),
emailfrom varchar(500),
emailto varchar(500),
CC varchar(500),
subject varchar(2000),
bodytext text,
attachmentfilenames varchar(2000),
msgfilename varchar(1000)
)


select *, convert(int, contactID)
from email
where contactID = 0.0

select count(*) from email

UPDATE email SET contactID = 0
UPDATE email SET companyID = 0