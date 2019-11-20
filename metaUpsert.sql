/****** Object:  StoredProcedure [Meta].[Upsert]    Script Date: 11/20/2019 7:46:01 AM ******/
DROP PROCEDURE [Meta].[Upsert]
GO

/****** Object:  StoredProcedure [Meta].[Upsert]    Script Date: 11/20/2019 7:46:01 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [Meta].[Upsert]
	@SourceSchema VARCHAR(250)
	,@SourceTable VARCHAR(250)
	,@TargetDatabase VARCHAR(250) = NULL
	,@TargetSchema VARCHAR(250)
	,@TargetTable VARCHAR(250)
	,@BatchId int
	,@Method VARCHAR(100)
	,@KeyColumns VARCHAR(4000)
	,@ExcludeHistoryForColumns VARCHAR(4000)
	,@PartialLoadIdentifierColumns VARCHAR(4000)
	,@CloseDeletedRows bit = 1
	,@PrintOnly bit = 0
AS
BEGIN
/*
	=============================================
	Author:			Espen Uhre Arnholtz, www.datageek.dk
	Create date:	2015-04-01, updated 2017-09-15
	Description:	Merging source table rows into target table using different types of history concepts
	=============================================
	
	Disclamer:
	----------------------
		This script has no warranty, it is provided "as is". It is your responsibility to validate the behavior of the routines and their accuracy using the source code provided.
		Please do not use in production before you have conducted thorough testing!

	Parameter explanation:
	----------------------
		@SourceSchema: Schema name of the source
		@SourceTable: Table name of the source
		@TargetDatabase: Database name of the target
		@TargetSchema: Schema name of the target
		@TargetTable: Table name of the target
		@BatchId: Log identifier of batch
		@Method: Method of upsert; Append, Snapshot or Type2
		@KeyColumns: Comma separated list of columns defining the key of the table
		@ExcludeHistoryForColumns: If using the Type2 Method, then define the columns, as a comma separated list, that should not trigger a history change (ex. a column that defines the load timestamp from source).
		@PartialLoadIdentifierColumns: When using the Snapshot or Type2 method, you can specify which columns are defining the scope of the load. When merging into the target, the target will be filtered on the available values for these columns from the source (ex. if you want to merge only a subset of the total data into the target table). This will prevent closing rows in the target table which does not represented by this key definition in the source table.
		@CloseDeletedRows: When set to 0, the rows in the target table will not be closed if they are identified as closed in source. Only to be used with Method = Type2.
		@PrintOnly: When set to 1 the procedure will only print the SQL statement and not execute.

	Method explanation:
	----------------------
		Append:
			All rows from the source will be added to the target. All rows in the target are inserted as open, no closing of previous rows.

		Snapshot:
			All rows from the source will be added to the target. All previous rows in the target will be closed.

		Type2:
			All rows from the source will be merged into the target. All previous rows in the target will be closed.

	Prerequisites:
	----------------------
		This SP requires the following columns in the target table:

			[DW_ValidFrom] as datetime
			[DW_ValidTo] as datetime
			[DW_BatchId] as int
			[DW_ClosedByBatchId] as int
	
		This SP requires the Table-valued Function Meta.fnSplit:

			CREATE FUNCTION [Meta].[fnSplit] (
				@sInputList VARCHAR(8000) -- List of delimited items
				,@sDelimiter VARCHAR(8000) = ',' -- delimiter that separates items
			)
			RETURNS @List TABLE (item VARCHAR(8000))

			BEGIN
				DECLARE @sItem VARCHAR(8000)
				WHILE CHARINDEX(@sDelimiter,@sInputList,0) <> 0
				BEGIN
					SELECT
						@sItem = RTRIM(LTRIM(SUBSTRING(@sInputList,1,CHARINDEX(@sDelimiter,@sInputList,0)-1))),
						@sInputList = RTRIM(LTRIM(SUBSTRING(@sInputList,CHARINDEX(@sDelimiter,@sInputList,0)+LEN(@sDelimiter),LEN(@sInputList))))
					IF LEN(@sItem) > 0
						INSERT INTO @List
						SELECT @sItem
				END

				IF LEN(@sInputList) > 0
					INSERT INTO @List
					SELECT @sInputList -- Put the last item in
				RETURN
			END

	Debug statement / example of use:
	----------------------
		EXECUTE [Meta].[Upsert]
		   @SourceSchema = 'Extract'
		  ,@SourceTable = 'Customer'
		  ,@TargetDatabase = NULL
		  ,@TargetSchema = 'ExtractHistory'
		  ,@TargetTable = 'Customer'
		  ,@BatchId = 0
		  ,@Method = 'Type2'
		  ,@KeyColumns = 'CustomerNumber'
		  ,@ExcludeHistoryForColumns = 'LastUpdatedTimestamp'
		  ,@PartialLoadIdentifierColumns = 'CompanyID'
		  ,@PrintOnly = 1
*/

	-- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	/* Set database to current if not set by parameter */
	IF @TargetDatabase IS NULL
	BEGIN
		SET @TargetDatabase = DB_NAME()
	END
	
	/* Validate Method */
	IF @Method NOT IN (
		'Append'
		,'Snapshot'
		,'Type2'
	)
	BEGIN
		RAISERROR ('The Method "%s" specified is not valid (specify either Append, Snapshot or Type2)'
		, 16, 1
		, @Method)
		RETURN
	END	
	
	/* Validate if source table exists */
	IF NOT EXISTS (
		SELECT
			[TABLE_NAME]
		FROM
			[INFORMATION_SCHEMA].[TABLES]
		WHERE
			[TABLE_SCHEMA] = @SourceSchema
			AND [TABLE_NAME] = @SourceTable
	)
	BEGIN
		RAISERROR ('The source table "%s" in schema "%s" does not exist'
		, 16, 1
		, @SourceTable, @SourceSchema)
		RETURN
	END

	/* Validate if target table exists */
	EXECUTE('
	IF NOT EXISTS (
		SELECT
			[TABLE_NAME]
		FROM
			[' + @TargetDatabase + '].[INFORMATION_SCHEMA].[TABLES]
		WHERE
			[TABLE_SCHEMA] = ''' + @TargetSchema + '''
			AND [TABLE_NAME] = ''' + @TargetTable + '''
	)
	BEGIN
		RAISERROR (''The target table "%s" in schema "%s" does not exist''
		, 16, 1
		, ''' + @TargetTable + ''', ''' + @TargetSchema + ''')
		RETURN
	END
	')

	/* Validate if all @KeyColumns exists in the source table */
	IF EXISTS (
		SELECT
			*
		FROM
			Meta.fnSplit(@KeyColumns,',') AS ColumsSpecified
			LEFT OUTER JOIN [INFORMATION_SCHEMA].[COLUMNS] AS ColumsExists
				ON ColumsExists.[TABLE_SCHEMA] = @SourceSchema
				AND ColumsExists.[TABLE_NAME] = @SourceTable
				AND ColumsExists.[COLUMN_NAME] = ColumsSpecified.item
		WHERE
			ColumsExists.[COLUMN_NAME] IS NULL
	)
	BEGIN
		RAISERROR ('The source table "%s" in schema "%s" does not contain all the key columns specified: "%s"'
		, 16, 1
		, @SourceTable, @SourceSchema, @KeyColumns)
		RETURN
	END

	/* Validate if all @KeyColumns exists in the target table */
	EXECUTE('
		IF EXISTS (
			SELECT
				*
			FROM
				Meta.fnSplit(''' + @KeyColumns + ''','','') AS ColumsSpecified
				LEFT OUTER JOIN [' + @TargetDatabase + '].[INFORMATION_SCHEMA].[COLUMNS] AS ColumsExists
					ON ColumsExists.[TABLE_SCHEMA] = ''' + @TargetSchema + '''
					AND ColumsExists.[TABLE_NAME] = ''' + @TargetTable + '''
					AND ColumsExists.[COLUMN_NAME] = ColumsSpecified.item
			WHERE
				ColumsExists.[COLUMN_NAME] IS NULL
		)
		BEGIN
			RAISERROR (''The target table "%s" in schema "%s" does not contain all the key column(s) specified: "%s"''
			, 16, 1
			, ''' + @TargetTable + ''', ''' + @TargetSchema + ''', ''' + @KeyColumns + ''')
			RETURN
		END
	')

	/* Validate if all @ExcludeHistoryForColumns exists in the source table */
	EXECUTE('
		IF EXISTS (
			SELECT
				*
			FROM
				Meta.fnSplit(''' + @ExcludeHistoryForColumns + ''','','') AS ColumsSpecified
				LEFT OUTER JOIN [' + @TargetDatabase + '].[INFORMATION_SCHEMA].[COLUMNS] AS ColumsExists
					ON ColumsExists.[TABLE_SCHEMA] = ''' + @TargetSchema + '''
					AND ColumsExists.[TABLE_NAME] = ''' + @TargetTable + '''
					AND ColumsExists.[COLUMN_NAME] = ColumsSpecified.item
			WHERE
				ColumsExists.[COLUMN_NAME] IS NULL
		)
		BEGIN
			RAISERROR (''The target table "%s" in schema "%s" does not contain all the exclude for history column(s) specified: "%s"''
			, 16, 1
			, ''' + @TargetTable + ''', ''' + @TargetSchema + ''', ''' + @ExcludeHistoryForColumns + ''')
			RETURN
		END
	')
	
	/* Validate if all @CloseDeletedRows is only used with Method = Type2 */
	IF (
		@CloseDeletedRows = 0
		AND @Method <> 'Type2'
	)
	BEGIN
		RAISERROR ('The @DoNotCloseDeletedRows parameter for table "%s" in schema "%s" can only be set to False when @Method is Type2'
		, 16, 1
		, @SourceTable, @SourceSchema)
		RETURN
	END

	/* Validate if all @PartialLoadIdentifierColumns exists in the target table */
	EXECUTE('
		IF EXISTS (
			SELECT
				*
			FROM
				Meta.fnSplit(''' + @PartialLoadIdentifierColumns + ''','','') AS ColumsSpecified
				LEFT OUTER JOIN [' + @TargetDatabase + '].[INFORMATION_SCHEMA].[COLUMNS] AS ColumsExists
					ON ColumsExists.[TABLE_SCHEMA] = ''' + @TargetSchema + '''
					AND ColumsExists.[TABLE_NAME] = ''' + @TargetTable + '''
					AND ColumsExists.[COLUMN_NAME] = ColumsSpecified.item
			WHERE
				ColumsExists.[COLUMN_NAME] IS NULL
		)
		BEGIN
			RAISERROR (''The target table "%s" in schema "%s" does not contain all the partial load identifier columns specified: "%s"''
			, 16, 1
			, ''' + @TargetTable + ''', ''' + @TargetSchema + ''', ''' + @PartialLoadIdentifierColumns + ''')
			RETURN
		END
	')

	/* Validate if any missing system columns */
	EXECUTE('
		IF (
			SELECT
				COUNT(*)
			FROM
				[' + @TargetDatabase + '].[INFORMATION_SCHEMA].[COLUMNS] AS ColumsExists
			WHERE
				[TABLE_SCHEMA] = ''' + @TargetSchema + '''
				AND [TABLE_NAME] = ''' + @TargetTable + '''
				AND [COLUMN_NAME] IN (''DW_ValidFrom'', ''DW_ValidTo'', ''DW_BatchId'', ''DW_ClosedByBatchId'')
		) < 4
		BEGIN
			RAISERROR (''Some mandatory system columns (DW_ValidFrom, DW_ValidTo, DW_BatchId, DW_ClosedByBatchId) are missing in the target table "%s" in schema "%s".''
			, 16, 1
			, ''' + @TargetTable + ''', ''' +  @TargetSchema + ''')
			RETURN
		END
	')

	/* Validate if @BatchId is set */
	IF (@BatchId IS NULL)
	BEGIN
		RAISERROR ('The @BatchId parameter is not set'
		, 16, 1)
		RETURN
	END

	DECLARE @ColumnMapping TABLE (
		[S_COLUMN_NAME] sysname
		,[T_COLUMN_NAME] sysname
		,[KeyColumns_ColumnName] sysname NULL
		,[ExcludeHistoryForColumns_ColumnName] sysname NULL
		,[PartialLoadIdentifierColumns_ColumnName] sysname NULL
	)

	INSERT INTO
		@ColumnMapping
	EXECUTE('
		SELECT
			[S_COLUMN_NAME] = S.COLUMN_NAME
			,[T_COLUMN_NAME] = T.COLUMN_NAME
			,[KeyColumns_ColumnName] = KeyColumns.ColumnName
			,[ExcludeHistoryForColumns_ColumnName] = ExcludeHistoryForColumns.ColumnName
			,[PartialLoadIdentifierColumns_ColumnName] = PartialLoadIdentifierColumns.ColumnName
		FROM
			[INFORMATION_SCHEMA].[COLUMNS] AS S
			INNER JOIN [' + @TargetDatabase + '].[INFORMATION_SCHEMA].[COLUMNS] AS T
				ON T.[COLUMN_NAME] = S.[COLUMN_NAME]
			LEFT OUTER JOIN (
				SELECT
					[ColumnName] = item
				FROM
					Meta.fnSplit(''' + @KeyColumns + ''','','')
			) AS KeyColumns
				ON KeyColumns.[ColumnName] = S.[COLUMN_NAME]
			LEFT OUTER JOIN (
				SELECT
					[ColumnName] = item
				FROM
					Meta.fnSplit(''' + @ExcludeHistoryForColumns + ''','','')
			) AS ExcludeHistoryForColumns
				ON ExcludeHistoryForColumns.[ColumnName] = S.[COLUMN_NAME]
			LEFT OUTER JOIN (
				SELECT
					[ColumnName] = item
				FROM
					Meta.fnSplit(''' + @PartialLoadIdentifierColumns + ''','','')
			) AS PartialLoadIdentifierColumns
				ON PartialLoadIdentifierColumns.[ColumnName] = S.[COLUMN_NAME]
		WHERE
			S.[TABLE_SCHEMA] = ''' + @SourceSchema + '''
			AND S.[TABLE_NAME] = ''' + @SourceTable + '''
			AND T.[TABLE_SCHEMA] = ''' + @TargetSchema + '''
			AND T.[TABLE_NAME] = ''' + @TargetTable + '''
		ORDER BY
			S.[ORDINAL_POSITION]
	')


	/* Set variables with meta data for the upsert statement */
	DECLARE
		@SqlToExecute VARCHAR(MAX)
		,@ColumnList VARCHAR(MAX)
		,@SourceColumnList VARCHAR(MAX)
		,@CompareCondition VARCHAR(MAX)
		,@MergeOnCondition VARCHAR(MAX)
		,@MergeOutConditionColumn VARCHAR(MAX)
		,@FilterTargetColumnList VARCHAR(MAX)
		,@FilterTargetOnCondition VARCHAR(MAX)
		,@BatchId_str VARCHAR(20);

	SET @BatchId_str = CAST(@BatchId AS varchar(20));

	SELECT
		-- All columns
		@ColumnList = COALESCE(@ColumnList + ', ', '') + '[' + T_COLUMN_NAME + ']'

		-- All columns, source prefix
		,@SourceColumnList = COALESCE(@SourceColumnList + ', ', '') + 'S.[' + T_COLUMN_NAME + ']'
		
		-- Defines the where condition including columns with data from tracking changes
		,@CompareCondition = (
			CASE
				WHEN ExcludeHistoryForColumns_ColumnName IS NULL AND KeyColumns_ColumnName IS NULL
				THEN COALESCE(@CompareCondition + ' OR ', '') + 'S.[' + S_COLUMN_NAME + '] <> T.[' + T_COLUMN_NAME + '] OR (S.[' + S_COLUMN_NAME + '] IS NULL AND T.[' + T_COLUMN_NAME + '] IS NOT NULL) OR (S.[' + S_COLUMN_NAME + '] IS NOT NULL AND T.[' + S_COLUMN_NAME + '] IS NULL)'
			ELSE
				@CompareCondition
			END
		)

		-- Defines the join / merge condition including key columns and partial load identifiers
		,@MergeOnCondition = (
			CASE
				WHEN KeyColumns_ColumnName IS NOT NULL OR PartialLoadIdentifierColumns_ColumnName IS NOT NULL
				THEN COALESCE(@MergeOnCondition + ' AND ', '') + '(S.[' + S_COLUMN_NAME + '] = T.[' + T_COLUMN_NAME + '] OR (S.[' + S_COLUMN_NAME + '] IS NULL AND T.[' + T_COLUMN_NAME + '] IS NULL))'
			ELSE
				@MergeOnCondition
			END
		)

		-- A technical column for the merge script, the first key column
		,@MergeOutConditionColumn = (
			CASE
				WHEN KeyColumns_ColumnName IS NOT NULL AND @MergeOutConditionColumn IS NULL
				THEN '[' + S_COLUMN_NAME + ']'
			ELSE
				@MergeOutConditionColumn
			END
		)

		-- Defines the scope of the merge as a column list for target filter
		,@FilterTargetColumnList = (
			CASE
				WHEN PartialLoadIdentifierColumns_ColumnName IS NOT NULL
				THEN COALESCE(@FilterTargetColumnList + ', ', '') + '[' + S_COLUMN_NAME + ']'
			ELSE
				@FilterTargetColumnList
			END
		)

		-- Defines the scope of the merge as a join condition between source and target
		,@FilterTargetOnCondition = (
			CASE
				WHEN PartialLoadIdentifierColumns_ColumnName IS NOT NULL
				THEN COALESCE(@FilterTargetOnCondition + ' AND ', '') + 'S.[' + S_COLUMN_NAME + '] = T.[' + T_COLUMN_NAME + ']'
			ELSE
				@FilterTargetOnCondition
			END
		)
	FROM
		@ColumnMapping


	/* Validate if any colums left for "update" */
	IF (@ColumnList IS NULL)
	BEGIN
		RAISERROR ('No matching (name) colums available between the source table "%s" in schema "%s" and the target table "%s" in schema "%s"'
		, 16, 1
		, @SourceTable, @SourceSchema, @TargetTable, @TargetSchema)
		RETURN
	END

	/* Validate if any wrong parameters filled if using the Append method */
	IF (@Method = 'Append' AND (COALESCE(@KeyColumns, '') != '' OR COALESCE(@ExcludeHistoryForColumns, '') != '' OR COALESCE(@PartialLoadIdentifierColumns, '') != ''))
	BEGIN
		RAISERROR ('When using the Append method the following parameters should be left blank / null: KeyColumns, ExcludeHistoryForColumns and PartialLoadIdentifierColumns. Related to source table "%s" in schema "%s" and target table "%s" in schema "%s".'
		, 16, 1
		, @SourceTable, @SourceSchema, @TargetTable, @TargetSchema)
		RETURN
	END

	/* Validate if any wrong parameters filled if using the Snapshot method */
	IF (@Method = 'Snapshot' AND (COALESCE(@KeyColumns, '') != '' OR COALESCE(@ExcludeHistoryForColumns, '') != ''))
	BEGIN
		RAISERROR ('When using the Snapshot method the following parameters should be left blank (or null): KeyColumns and ExcludeHistoryForColumns. Related to source table "%s" in schema "%s" and target table "%s" in schema "%s".'
		, 16, 1
		, @SourceTable, @SourceSchema, @TargetTable, @TargetSchema)
		RETURN
	END

	/* Validate if any missing parameters if using the Type2 method */
	IF (@Method = 'Type2' AND (COALESCE(@KeyColumns, '') = ''))
	BEGIN
		RAISERROR ('When using the Type2 method you need to specify one or more key colums for the source table "%s" in schema "%s" and target table "%s" in schema "%s".'
		, 16, 1
		, @SourceTable, @SourceSchema, @TargetTable, @TargetSchema)
		RETURN
	END

	/* Define upsert statement for each Method */

	/* Statement for method: Append */
	IF (@Method = 'Append')
	BEGIN
		SET @SqlToExecute =
	'
	DECLARE @ExecutionDateTime datetime2;
	DECLARE @JustBeforeExecutionDateTime datetime2;
	SET @ExecutionDateTime = SYSDATETIME();
	SET @JustBeforeExecutionDateTime = DATEADD(ms, -3, @ExecutionDateTime);
                 
	INSERT INTO [' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + ']
	(
		' + @ColumnList + '
		,[DW_ValidFrom]
		,[DW_ValidTo]
		,[DW_BatchId]
	)
	SELECT
		' + @ColumnList + '
		,[DW_ValidFrom] = @ExecutionDateTime
		,[DW_ValidTo] = ''9999-12-31''
		,[DW_BatchId] = ''' + @BatchId_str + '''
	FROM
		[' + @SourceSchema + '].[' + @SourceTable + ']
	'
	END
                 
	/* Statement for method: Snapshot */
	IF (@Method = 'Snapshot')
	BEGIN
		SET @SqlToExecute =
	'
	DECLARE @ExecutionDateTime datetime2;
	DECLARE @JustBeforeExecutionDateTime datetime2;
	SET @ExecutionDateTime = SYSDATETIME();
	SET @JustBeforeExecutionDateTime = DATEADD(ms, -3, @ExecutionDateTime);
	' + (
		CASE WHEN @FilterTargetColumnList IS NOT NULL
		THEN '
	WITH T AS (
		SELECT
			T.*
		FROM
			[' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + '] AS T
			INNER JOIN (
				SELECT
					' + @FilterTargetColumnList + '
				FROM
					[' + @SourceSchema + '].[' + @SourceTable + ']
				GROUP BY
					' + @FilterTargetColumnList + '
			) AS S
				ON ' + @FilterTargetOnCondition + '
	)
	UPDATE T
	'
		ELSE
	'
	UPDATE [' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + ']
	'
		END
	)
	+ 'SET
		[DW_ValidTo] = @JustBeforeExecutionDateTime
		,[DW_ClosedByBatchId] = ''' + @BatchId_str + '''
	WHERE
		[DW_ValidTo] = ''9999-12-31''

	INSERT INTO [' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + ']
	(
		' + @ColumnList + '
		,[DW_ValidFrom]
		,[DW_ValidTo]
		,[DW_BatchId]
	)
	SELECT
		' + @ColumnList + '
		,[DW_ValidFrom] = @ExecutionDateTime
		,[DW_ValidTo] = ''9999-12-31''
		,[DW_BatchId] = ''' + @BatchId_str + '''
	FROM
		[' + @SourceSchema + '].[' + @SourceTable + ']
	'
	END

	/* Statement for method: Type2. If business key is defined, but no compare condition (all columns are keys) */
	IF (@Method = 'Type2' AND @CompareCondition IS NULL)
	BEGIN
		SET @SqlToExecute =
	'
	DECLARE @ExecutionDateTime datetime2;
	DECLARE @JustBeforeExecutionDateTime datetime2;
	SET @ExecutionDateTime = SYSDATETIME();
	SET @JustBeforeExecutionDateTime = DATEADD(ms, -3, @ExecutionDateTime);
	' + (
		CASE WHEN @FilterTargetColumnList IS NOT NULL
		THEN '
	WITH T AS (
		SELECT
			T.*
		FROM
			[' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + '] AS T
			INNER JOIN (
				SELECT
					' + @FilterTargetColumnList + '
				FROM
					[' + @SourceSchema + '].[' + @SourceTable + ']
				GROUP BY
					' + @FilterTargetColumnList + '
			) AS S
				ON ' + @FilterTargetOnCondition + '
	)
	MERGE T
	'
		ELSE
	'
	MERGE [' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + '] AS T
	'
		END
	)
	
	+ 'USING [' + @SourceSchema + '].[' + @SourceTable + '] AS S
	ON (
		' + @MergeOnCondition + '
		AND T.[DW_ValidTo] = ''9999-12-31''
	)
	WHEN NOT MATCHED THEN
	INSERT (
		' + @ColumnList + '
		,[DW_ValidFrom]
		,[DW_ValidTo]
		,[DW_BatchId]
	)
	VALUES (
		' + @ColumnList + '
		,@ExecutionDateTime
		,''9999-12-31''
		,''' + @BatchId_str + '''
	)
	' + (
		CASE WHEN @CloseDeletedRows = 1
		THEN
	'
	WHEN NOT MATCHED BY SOURCE AND (
		[DW_ValidTo] = ''9999-12-31''
	)
	THEN
		UPDATE SET
			[DW_ValidTo] = @JustBeforeExecutionDateTime
			,[DW_ClosedByBatchId] = ''' + @BatchId_str + ''';
	'
		ELSE
			';'
		END
	)
	END

	/* Statement for method: Type2. If business key is and compare condition is defined */
	IF (@Method = 'Type2' AND @CompareCondition IS NOT NULL)
	BEGIN
		SET @SqlToExecute =
	'
	DECLARE @ExecutionDateTime datetime2;
	DECLARE @JustBeforeExecutionDateTime datetime2;
	SET @ExecutionDateTime = SYSDATETIME();
	SET @JustBeforeExecutionDateTime = DATEADD(ms, -3, @ExecutionDateTime);
	' + (
		CASE WHEN @FilterTargetColumnList IS NOT NULL
		THEN '
	WITH T AS (
		SELECT
			T.*
		FROM
			[' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + '] AS T
			INNER JOIN (
				SELECT
					' + @FilterTargetColumnList + '
				FROM
					[' + @SourceSchema + '].[' + @SourceTable + ']
				GROUP BY
					' + @FilterTargetColumnList + '
			) AS S
				ON ' + @FilterTargetOnCondition + '
	)
	'
		ELSE
			''
		END
	)
	+ '
	INSERT INTO [' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + ']
	(
		' + @ColumnList + '
		,[DW_ValidFrom]
		,[DW_ValidTo]
		,[DW_BatchId]
	)
	SELECT
		' + @ColumnList + '
		,[DW_ValidFrom] = @ExecutionDateTime
		,[DW_ValidTo] = ''9999-12-31''
		,[DW_BatchId] = ''' + @BatchId_str + '''
	FROM (
		' + (
			CASE WHEN @FilterTargetColumnList IS NOT NULL
			THEN '
		MERGE T
		'
			ELSE
		'
		MERGE [' + @TargetDatabase + '].[' + @TargetSchema + '].[' + @TargetTable + '] AS T
		'
			END
		)	
		+ 'USING [' + @SourceSchema + '].[' + @SourceTable + '] AS S
		ON (
			' + @MergeOnCondition + '
			AND T.[DW_ValidTo] = ''9999-12-31''
		)
		WHEN NOT MATCHED THEN
		INSERT (
			' + @ColumnList + '
			,[DW_ValidFrom]
			,[DW_ValidTo]
			,[DW_BatchId] 
		)
		VALUES (
			' + @ColumnList + '
			,@ExecutionDateTime
			,''9999-12-31''
			,''' + @BatchId_str + '''
		)
		WHEN MATCHED AND (
			' + @CompareCondition + '
		)
		THEN
			UPDATE SET
				[DW_ValidTo] = @JustBeforeExecutionDateTime
				,[DW_ClosedByBatchId] = ''' + @BatchId_str + '''
		' + (
			CASE WHEN @CloseDeletedRows = 1
			THEN
		'
		WHEN NOT MATCHED BY SOURCE AND (
			[DW_ValidTo] = ''9999-12-31''
		)
		THEN
			UPDATE SET
				[DW_ValidTo] = @JustBeforeExecutionDateTime
				,[DW_ClosedByBatchId] = ''' + @BatchId_str + '''
		'
			ELSE
				''
			END
		)
		+ 'OUTPUT
			' + @SourceColumnList + '
			,$action AS Action_Out
	) AS MERGE_OUT
	WHERE
		MERGE_OUT.Action_Out = ''UPDATE''
		AND ' + @MergeOutConditionColumn + ' IS NOT NULL
	;
	'
	END

	IF (@PrintOnly = 1)
	BEGIN
	   --CREATE TABLE TestTable
	   --(
	   --Query VARCHAR(MAX)
	   --)

	   --TRUNCATE TABLE TestTable
	   --INSERT INTO dbo.TestTable VALUES (@SqlToExecute);
	   --DROP TABLE dbo.TestTable
	   --SELECT @SqlToExecute; 
		PRINT @SqlToExecute;
	END
	ELSE
	BEGIN TRY
		BEGIN TRANSACTION
			EXEC (@SqlToExecute);
		COMMIT
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			ROLLBACK

		DECLARE @ErrorMessage AS varchar(4000);
		SET @ErrorMessage = ERROR_MESSAGE();

		RAISERROR ('Error updating table "%s" in schema "%s" in database "%s" from source table "%s" in schema "%s": %s'
		, 16, 1
		, @TargetTable, @TargetSchema, @TargetDatabase, @SourceTable, @SourceSchema, @ErrorMessage)
		RETURN
	END CATCH
END

GO


