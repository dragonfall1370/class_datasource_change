create table Temp_CandidateCustomFields 
(
candidateExternalId character varying(100),
additionalType character varying(20),
formId integer,
fieldIdPublicationDate integer,
fieldIdEducation integer,
fieldIdCareerLevel integer,
insertTimeStamp timestamp without time zone,
EducationValue character varying,
fieldDateValue timestamp without time zone,
CareerLevelValue character varying,
vcCandidateId bigint
);