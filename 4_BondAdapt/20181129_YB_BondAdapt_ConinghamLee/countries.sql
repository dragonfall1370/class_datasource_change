--USE [BULLHORN]
GO
/****** Object:  Table [dbo].[tmp_country]    Script Date: 12/22/2016 11:34:12 AM ******/
DROP TABLE [dbo].[tmp_country]
GO
/****** Object:  Table [dbo].[tmp_country]    Script Date: 12/22/2016 11:34:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[tmp_country](
	[COUNTRY] [varchar](50) NULL,
	[ABBREVIATION] [varchar](50) NULL,
	[CODE] [varchar](50) NULL
) --ON [BULLHORN_DATA]

GO
SET ANSI_PADDING OFF
GO
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Bahamas', N'BS', N'BAH')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Democratic Republic of the Congo', N'CG', N'DRC')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Ghana', N'GH', N'GHA')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Namibia', N'NA', N'NAM')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Nigeria', N'NG', N'NIG')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'South Africa', N'ZA', N'SAF')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Saudi Arabia', N'SA', N'SAR')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'United States', N'US', N'USA')
INSERT [dbo].[tmp_country] ([COUNTRY], [ABBREVIATION], [CODE]) VALUES (N'Zambia', N'ZM', N'ZAM')

