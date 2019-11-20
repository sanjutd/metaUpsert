
/****** Object:  Table [run].[metaUpsertControl]    Script Date: 11/20/2019 7:48:54 AM ******/
DROP TABLE [run].[metaUpsertControl]
GO

/****** Object:  Table [run].[metaUpsertControl]    Script Date: 11/20/2019 7:48:54 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [run].[metaUpsertControl](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[SourceSchema] [varchar](200) NOT NULL,
	[SourceTable] [varchar](200) NOT NULL,
	[TargetDatabase] [varchar](200) NULL,
	[TargetSchema] [varchar](200) NOT NULL,
	[TargetTable] [varchar](200) NOT NULL,
	[Method] [varchar](200) NOT NULL,
	[KeyColumns] [varchar](200) NOT NULL,
	[ExcludeHistoryForColumns] [varchar](200) NULL,
	[PartialLoadIdentifierColumns] [varchar](200) NULL,
	[PrintOnly] [varchar](200) NULL,
	[CloseDeletedRows] [int] NULL,
	[IsMetaUpsertOnly] [int] NULL,
	[ParentWebserviceName] [varchar](200) NOT NULL,
	[IsActive] [int] NOT NULL
) ON [PRIMARY]

GO


