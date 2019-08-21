CREATE TABLE [dbo].[Candidate] (
[DynamicDataId] int,
[LastUpdatedByuserId] int,
[LastUpdatedBy] nvarchar(max),
[LastUpdatedDate] date,
[VCTitle] nvarchar(max),
[Name] nvarchar(max),
[FirstName] nvarchar(max),
[LastName] nvarchar(max),
[Nationality] nvarchar(max),
[HomeTel] nvarchar(max),
[Mobile] nvarchar(max),
[Email] nvarchar(max),
[Source] nvarchar(max),
[CurrentCompany] nvarchar(max),
[Salary] int,
[CurrentJobTitle] nvarchar(max),
[Started] date,
[ResponsibilitiesandAchievements] nvarchar(max),
[RolesHistory] nvarchar(max),
[InterviewNotes] nvarchar(max),
[Locations] nvarchar(max),
[Qualifications] nvarchar(max),
[JobFunction] nvarchar(max),
[Industry] nvarchar(max),
[Add1] nvarchar(max),
[Add2] nvarchar(max),
[Town] nvarchar(max),
[HomeProvince] nvarchar(max),
[Postcode] nvarchar(max),
[LinkedIn] nvarchar(max),
[DateofBirth] date,
[WorkPermit] nvarchar(max),
[FixedTermContract] int,
[Contract] int,
[Permanent] int,
[Email2] nvarchar(max),
[City] nvarchar(max),
[Mobile2] nvarchar(max),
[CreatedDate] nvarchar(max),
[IndustrySkills] nvarchar(max),
[JobFunctions] nvarchar(max)
)


CREATE TABLE [dbo].[Contact] (
[DynamicDataId] int,
[LastUpdatedByuserId] int,
[LastUpdatedByEmail] nvarchar(max),
[LastUpdatedDate] date,
[Name] nvarchar(max),
[FirstName] nvarchar(max),
[LastName] nvarchar(max),
[Company] nvarchar(max),
[CompanyID] nvarchar(max),
[Tel] nvarchar(max),
[Mobile] nvarchar(max),
[Mobile2] nvarchar(max),
[MobileCombine] nvarchar(max),
[Fax] nvarchar(max),
[Email] nvarchar(max),
[Email2] nvarchar(max),
[EmailCombine] nvarchar(max),
[VCTitle] nvarchar(max),
[Position] nvarchar(max),
[Extn] nvarchar(max),
[DoNotCall] int,
[knownAs] nvarchar(max),
[Division] nvarchar(max),
[CreatedDate] nvarchar(max)
)

CREATE TABLE [dbo].[Vacancy] (
[DynamicDataId] int,
[LastUpdatedByuserId] int,
[LastUpdatedByEmail] nvarchar(max),
[LastUpdatedDate] date,
[JobTitle] nvarchar(max),
[Consultant] nvarchar(max),
[Company] nvarchar(max),
[CompanyId] int,
[Contact] nvarchar(max),
[ContactId] int,
[Status] nvarchar(max),
[VacancyType] nvarchar(max),
[Requirements] nvarchar(max),
[SkillSet] nvarchar(max),
[JobSpecNotes] nvarchar(max),
[Other] nvarchar(max),
[Source] nvarchar(max),
[NumberofPositions] int,
[JobType] nvarchar(max),
[RateTo] nvarchar(max),
[CurrencyCode] nvarchar(max),
[IssuedOn] nvarchar(max),
[Country] nvarchar(max),
[BuildingName] nvarchar(max),
[Add1] nvarchar(max),
[Add2] nvarchar(max),
[Town] nvarchar(max),
[PostCode] int,
[ContractLength] nvarchar(max),
[ContractRate] nvarchar(max),
[Fee] nvarchar(max),
[qa_checkedBy] nvarchar(max),
[qa_dateChecked] date,
[qa_info] nvarchar(max),
[CreatedDate] nvarchar(max)
)

CREATE TABLE [dbo].[Company] (
[DynamicDataId] int,
[LastUpdatedByuserId] int,
[LastUpdatedByEmail] nvarchar(max),
[LastUpdatedDate] date,
[CompanyName] nvarchar(max),
[OfficeTelNo] nvarchar(max),
[Website] nvarchar(max),
[Add1] nvarchar(max),
[Add2] nvarchar(max),
[Town] nvarchar(max),
[Country] nvarchar(max),
[Postcode] nvarchar(max),
[Division] nvarchar(max),
[Industry] nvarchar(max),
[NoofEmp] nvarchar(max),
[AgreedRate] nvarchar(max),
[TermsSent] nvarchar(max),
[TermsSigned] nvarchar(max),
[AdditionalInfo] nvarchar(max),
[BuildingName] nvarchar(max),
[CompanyRegNo] nvarchar(max),
[AccPayableContact] nvarchar(max),
[FurtherInfo] nvarchar(max),
[alt_building] nvarchar(max),
[altAdd2] nvarchar(max),
[altTown] nvarchar(max),
[altAdd1] nvarchar(max),
[altCountry] nvarchar(max),
[altPostcode] nvarchar(max),
[Status] nvarchar(max),
[CreatedDate] nvarchar(max)
)


CREATE TABLE [dbo].[Note] (
[Id] int,
[ParentId] int,
[Text] nvarchar(max),
[CreatedBy] int,
[CreatedByEmail] nvarchar(max),
[CreatedDate] date
)


CREATE TABLE [dbo].[Users] (
[Id] int,
[EmailAddress] nvarchar(max),
[FirstName] nvarchar(max),
[LastName] nvarchar(max),
[ActiveIndicator] int
)

CREATE TABLE [dbo].[Document] (
[Id] int,
[Folder] nvarchar(max),
[Filename] nvarchar(max),
[Description] nvarchar(max),
[CreatedByUserId] int,
[CreatedByEmail] nvarchar(max),
[DynamicDataId] int,
[PrimaryIndicator] int,
[CreatedDate] date
)


CREATE TABLE [dbo].[Interview] (
[DynamicDataId] int,
[LastUpdatedByuserId] int,
[LastUpdatedByEmail] nvarchar(max),
[LastUpdatedDate] date,
[Reference] nvarchar(max),
[Candidate] nvarchar(max),
[CandidateID] int,
[Vacancy] nvarchar(max),
[VacancyID] int,
[Consultant] nvarchar(max),
[Interviewdate] nvarchar(max),
[Status] nvarchar(max),
[LoggedBy] nvarchar(max),
[Outcome] nvarchar(max),
[time_hh] int,
[time_mm] int,
[CreatedDate] nvarchar(max)
)