USE [RiskManagement]
GO

/****** Object:  StoredProcedure [run].[MetaUpsertCall]    Script Date: 11/20/2019 7:48:11 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROC [run].[MetaUpsertCall]
	@TableName NVARCHAR(400)
	,@Debug NCHAR(1) = 'N'
	,@Rows         INT           = NULL OUTPUT
AS
	BEGIN
		 SET NOCOUNT ON;
		 DECLARE @AuditID INT;
         DECLARE @SQLCmd NVARCHAR(MAX);
         DECLARE @ErrorMsg NVARCHAR(MAX) = CHAR(13) + CHAR(10);
         DECLARE @Error BIT = 0;
         DECLARE @ProcName NVARCHAR(128) = OBJECT_NAME(@@PROCID)
		 DECLARE @CurrBatchID int, @BatchId int, @loopCounter int= 1;
		 DECLARE @SourceSchema VARCHAR(250),@SourceTable VARCHAR(250)	,@TargetDatabase VARCHAR(250) = NULL	,@TargetSchema VARCHAR(250)	,@TargetTable VARCHAR(250)		,@Method VARCHAR(100)	,@KeyColumns VARCHAR(4000)
	,@ExcludeHistoryForColumns VARCHAR(4000)	,@PartialLoadIdentifierColumns VARCHAR(4000)	,@CloseDeletedRows NCHAR(1) ='1'	,@PrintOnly NCHAR(1) = '0'	,@TableCount bigint = 0
		 /*Parameter Validation*/
		 IF ISNULL(@TableName, '') = ''           
             BEGIN
                 SET @Error = 1;
                 SET @ErrorMsg = @ErrorMsg+'Parameters missing'+CHAR(13)+CHAR(10);
         END;
		 
		 IF NOT EXISTS
         (
             SELECT SourceTable
             FROM run.metaUpsertControl
             WHERE SourceTable = @TableName 
				and IsActive = 1 
         )
             BEGIN
                 SET @Error = 1;
                 SET @ErrorMsg = @ErrorMsg+'TableName {'+@TableName+'} does not exist'+CHAR(13)+CHAR(10);
         END;
		 SET @CurrBatchID = 0;
		 IF @Error = 0
             BEGIN
				SELECT @CurrBatchID= max(batchId) from run.batchLoad;

				
				SET @CurrBatchID=@CurrBatchID+1;

				 SET @SqlCmd = '';
				 
                 --SET @SqlCmd = @SqlCmd+'SELECT' +@CurrBatchID+ '= max(batchId) from run.batchLoad;  '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+'INSERT INTO  run.batchLoad (batchId, tableName) values (CAST('+cast(@CurrBatchID AS nvarchar(10))+' as INT),'''+@TableName+''') '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' DECLARE @SourceSchema VARCHAR(250),@SourceTable VARCHAR(250)	,@TargetDatabase VARCHAR(250) = NULL	,@TargetSchema VARCHAR(250)	,@TargetTable VARCHAR(250)		,@Method VARCHAR(100)	,@KeyColumns VARCHAR(4000)
	,@ExcludeHistoryForColumns VARCHAR(4000)	,@PartialLoadIdentifierColumns VARCHAR(4000)	,@CloseDeletedRows NCHAR(1) =''1''	,@PrintOnly NCHAR(1) = ''0''	,@TableCount bigint = 0 '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' select @SourceSchema = SourceSchema '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@SourceTable = SourceTable '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@TargetDatabase= TargetDatabase '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@TargetSchema= TargetSchema '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@TargetTable= TargetTable '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@Method= Method '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@KeyColumns= KeyColumns '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@ExcludeHistoryForColumns= ExcludeHistoryForColumns '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@PartialLoadIdentifierColumns= PartialLoadIdentifierColumns '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@CloseDeletedRows= CloseDeletedRows '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' ,@PrintOnly= PrintOnly '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' from run.metaUpsertControl where SourceTable = '''+@TableName +''' and IsActive = 1  '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+' EXECUTE [Meta].[Upsert]@SourceSchema,@SourceTable ,@TargetDatabase,@TargetSchema ,@TargetTable ,'+cast(@CurrBatchID as NCHAR(30))+',@Method ,@KeyColumns ,@ExcludeHistoryForColumns ,@PartialLoadIdentifierColumns,@CloseDeletedRows,@PrintOnly '+CHAR(13)+CHAR(10)
				 SET @SqlCmd = @SqlCmd+'update [run].[batchLoad] set stopdatetime = getdate() where batchid =  (select top 1 batchid from [run].[batchLoad] where tablename ='''+@TableName+'''order by batchid desc)'
				 				
                 IF @Debug = 'Y'
                     BEGIN
                         SELECT @SqlCmd;
                 END;
                     ELSE
                     BEGIN
                         BEGIN TRY
						  PRINT 'Load Entry into Batch Table';
                             EXECUTE [sp_executesql]
                                     @stmt = @SqlCmd;                                               
                             SET @Rows = @@ROWCOUNT;
						  PRINT 'Load Complete';
                 END TRY
                         BEGIN CATCH
                             SET @Error = 1;
                             SET @ErrorMsg = @ErrorMsg + ERROR_MESSAGE();
							 PRINT @ErrorMsg
                 END CATCH;
				 
			 END
		 /*New entry into the BATCH table*/
		
		
		END
	END 
--select * from run.[metaUpsertControl]

--EXEC [run].[MetaUpsertCall] 'ActivityEntityObjectives', 'Y'


 --update run.[metaUpsertControl] set TargetTable = 'ObjectivesH' where ID = 3 










GO


