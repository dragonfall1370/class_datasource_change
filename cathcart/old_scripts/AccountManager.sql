DROP TABLE [dbo].[AccountManager]
GO
/****** Object:  Table [dbo].[tmp_country]    Script Date: 12/22/2016 11:34:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[AccountManager](
	[user] [varchar](50) NULL,
	[fullname] [varchar](250) NULL,
	[email] [varchar](250) NULL
)

GO
SET ANSI_PADDING OFF
GO

--NM,KC,BK,ADM,NAPO,TJ,SUWI,NABU then 'nick.macdougall@cathcartassociates.com' --Nick Macdougall 
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'NM', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'KC', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'BK', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'ADM', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'NAPO', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'TJ', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'SUWI', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'NABU', N'Nick Macdougall', N'nick.macdougall@cathcartassociates.com')

--JIPR then jidapa.p@cathcartassociates.com --Jidapa Prakitrittanon
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'JIPR', N'Jidapa Prakitrittanon', N'jidapa.p@cathcartassociates.com')

--CHPR chaiwat.p@cathcartassociates.com --Chaiwat Prungsukarn
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'CHPR', N'Chaiwat Prungsukarn', N'chaiwat.p@cathcartassociates.com')

--SIVA sittikarn.v@cathcartassociates.com --Sittikarn Valee Ittikul
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'SIVA', N'Sittikarn Valee Ittikul', N'sittikarn.v@cathcartassociates.com')

--THKA thakaewlaow.k@cathcartassociats.com --Thakaewklaow Kaewmungkhala
INSERT [dbo].[AccountManager] ([user], [fullname], [email]) VALUES (N'THKA', N'Thakaewklaow Kaewmungkhala', N'thakaewlaow.k@cathcartassociats.com')

