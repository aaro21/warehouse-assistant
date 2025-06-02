DROP TABLE IF EXISTS [aud].[table_source];
DROP TABLE IF EXISTS [aud].[table_map];
DROP TABLE IF EXISTS [aud].[proc_metadata];
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [aud].[table_map](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[proc_id] [int] NULL,
	[dest_db] [nvarchar](100) NULL,
	[dest_schema] [nvarchar](100) NULL,
	[dest_table] [nvarchar](100) NULL,
	[record_insert_datetime] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [aud].[table_map] ADD PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [aud].[table_map] ADD  DEFAULT (getdate()) FOR [record_insert_datetime]
GO
ALTER TABLE [aud].[table_map]  WITH CHECK ADD FOREIGN KEY([proc_id])
REFERENCES [aud].[proc_metadata] ([id])
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [aud].[table_source](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[table_map_id] [int] NULL,
	[src_db] [nvarchar](100) NULL,
	[src_schema] [nvarchar](100) NULL,
	[src_table] [nvarchar](100) NULL,
	[role] [varchar](20) NULL,
	[join_predicate] [nvarchar](max) NULL,
	[record_insert_datetime] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [aud].[table_source] ADD PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [aud].[table_source] ADD  DEFAULT (getdate()) FOR [record_insert_datetime]
GO
ALTER TABLE [aud].[table_source]  WITH CHECK ADD FOREIGN KEY([table_map_id])
REFERENCES [aud].[table_map] ([id])
GO

CREATE TABLE [aud].[proc_metadata](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[proc_name] [nvarchar](300) NULL,
	[proc_hash] [nvarchar](100) NULL,
	[proc_definition] [nvarchar](max) NULL,
	[record_insert_datetime] [datetime] NULL,
	[source_db] [nvarchar](100) NULL,
	[source_schema] [nvarchar](100) NULL,
	[source_table] [nvarchar](100) NULL
) ON [PRIMARY]
GO
ALTER TABLE [aud].[proc_metadata] ADD PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [aud].[proc_metadata] ADD  DEFAULT (getdate()) FOR [record_insert_datetime]
GO


CREATE TABLE [aud].[column_map] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [table_source_id] INT NOT NULL,
    [dest_column] NVARCHAR(200) NOT NULL,
    [src_column] NVARCHAR(200) NOT NULL,
    [transform_expr] NVARCHAR(MAX) NULL,
    [record_insert_datetime] DATETIME NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [aud].[column_map] ADD PRIMARY KEY CLUSTERED 
(
    [id] ASC
) WITH (
    PAD_INDEX = OFF,
    STATISTICS_NORECOMPUTE = OFF,
    SORT_IN_TEMPDB = OFF,
    IGNORE_DUP_KEY = OFF,
    ONLINE = OFF,
    ALLOW_ROW_LOCKS = ON,
    ALLOW_PAGE_LOCKS = ON,
    OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
) ON [PRIMARY]
GO

ALTER TABLE [aud].[column_map] ADD  DEFAULT (getdate()) FOR [record_insert_datetime]
GO

ALTER TABLE [aud].[column_map]  WITH CHECK ADD FOREIGN KEY([table_source_id])
REFERENCES [aud].[table_source] ([id])
GO
