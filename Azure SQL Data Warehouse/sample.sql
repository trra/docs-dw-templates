IF OBJECT_ID ('[testSchema].[spStatus]', 'P') IS NOT NULL DROP PROCEDURE [testSchema].[spStatus];
GO

CREATE PROCEDURE [testSchema].[spStatus]
    @sessionNo   varchar(50),
    @packageName varchar(100),
    @scdDate     date
AS
-- ********** Project: DNZ DW Development v.1.2 **********
-- Git FileName: \3NF\SCD2 procedures\spStatus.sql
-- Fetch data from source [testSchema].[sStatus] into 3NF SCD Type 2 table [testSchema].[Status]
--
-- Execution sample:
-- EXEC [testSchema].[spStatus] 'adminSession', 'ETL', NULL
-- 
-- Author: Serge Artishev
-- Created: 2019-09-24 10:11:53 pm
-- Modified: SA 2019-09-24 10:11:53 pm
-- 
-- HISTORY:
-- PR     	Date      	By	Comments
-- ----   	----------	---	---------------------------------------------------------
-- T      	2019-06-04	SA	updated scd2 unit tests and set sessionStatus in exception block
-- T      	2019-05-06	SA	unified [dw] and [ods] column names 
--
SET NOCOUNT ON;
DECLARE @objectCode varchar(250) = 'testSchema.Status' -- define destination object code 
DECLARE @procName varchar(250)   = 'testSchema.spStatus' -- current proc name - OBJECT_NAME(@@PROCID) is not yet supported

DECLARE @rowsRead INT, @rowsWritten INT
DECLARE @message varchar(1000)
DECLARE @sessionStatus varchar(50)

PRINT replicate('-', 20) + ' EXECUTE [' + @procName + '] ' + replicate('-', 20)
BEGIN TRY
    -- add all manual dependencies here
    EXEC [etl].[addDependency] @sessionNo, @packageName, @srcObjectCode = 'testSchema.sStatus', @destObjectCode = @objectCode
    -- always check for session status AFTER adding new dependencies
    EXEC [etl].[checkSessionStatus] @sessionNo, @packageName, @objectCode, @sessionStatus OUT

    IF @sessionStatus in ('Ready', 'Failed') BEGIN 
        EXEC [etl].[setSessionStatus] @sessionNo, @packageName, @objectCode, @status = 'Running', @countRows = NULL
        -- ************ ETL code starts here ****************
        -- perform action
        
        DECLARE @now datetime = ndw.getLocalDateTime(getUtcDate())
        SET @scdDate = isNull(@scdDate, @now)

        IF OBJECT_ID('[testSchema].[Status]', 'U') IS NULL
        BEGIN
          SET @message = 'Create table [testSchema].[Status]'
          CREATE TABLE [testSchema].[Status] (
              [StatusKey] [bigint] IDENTITY(1,1) NOT NULL,
              [AssetKey] [bigint] NOT NULL,
              [AssetSource] [varchar](50) NOT NULL, -- PK
              [AssetNumber] [varchar](50) NOT NULL,
              [StatusCode] [varchar](50) NOT NULL, -- PK
              [StatusAttribute] [varchar](50) NULL,
              scdCurrentFlag [varchar](1) NULL,
              scdStartDate [datetime] NULL,
              scdEndDate [datetime] NULL,
              scdVersion [int] NULL,
              scdDeletedFlag [varchar](1) NULL,
              dwSessionNo [varchar](50) NULL,
              dwPackageName [varchar](100) NULL,
              dwUpdateDate [datetime] NULL
          ) WITH ( DISTRIBUTION = ROUND_ROBIN, HEAP )
          PRINT @message
          EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @message
        END

        IF (SELECT count(*) FROM [testSchema].[Status] WHERE [StatusKey] = -1) = 0
        BEGIN 
          SET @message = 'Insert default -1 "Unknown" row'
          SET IDENTITY_INSERT [testSchema].[Status] ON;
          INSERT INTO [testSchema].[Status]
            ( [StatusKey], [AssetKey],
              [AssetSource], [AssetNumber], [StatusCode], [StatusAttribute], 
              
              scdCurrentFlag, scdStartDate, scdEndDate, scdVersion, scdDeletedFlag, 
              dwSessionNo, dwPackageName, dwUpdateDate )
          SELECT 
              [StatusKey] = -1, 
              [AssetKey] = -1, 
              [AssetSource] ='UNKNOWN', 
              [AssetNumber] ='UNKNOWN', 
              [StatusCode] ='UNKNOWN', 
              [StatusAttribute] ='UNKNOWN',
              scdCurrentFlag = 'Y',
              scdStartDate = '1990-01-01 00:00:00.000',
              scdEndDate = '9999-12-31 00:00:00.000',
              scdVersion = 1,
              scdDeletedFlag = 'N',
              dwSessionNo = @sessionNo,
              dwPackageName = @packageName,
              dwUpdateDate = @now;
          SET IDENTITY_INSERT  [testSchema].[Status] OFF
          PRINT @message
          EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @message
        END
     
        PRINT '-- 2.0 Create temp source table with foreign key(s)'
        IF OBJECT_ID('tempdb..[#ds2.0.testSchema.sStatus]') IS NOT NULL DROP TABLE [#ds2.0.testSchema.sStatus]
        CREATE TABLE [#ds2.0.testSchema.sStatus] WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
        AS SELECT 
            [AssetKey] = a.[AssetKey],
            [AssetSource] = src.[AssetSource], [AssetNumber] = src.[AssetNumber], 
            [StatusCode] = src.[StatusCode], [StatusAttribute] = src.[StatusAttribute]
        FROM [testSchema].[sStatus] src 
        JOIN testSchema.Asset a 
          ON src.AssetSource = a.AssetSource AND src.AssetNumber = a.AssetNumber
          AND @scdDate between a.scdStartDate and a.scdEndDate
          

        PRINT '-- 2.1 Create temp table to update existing records'
        IF OBJECT_ID('tempdb..[#ds2.1.testSchema.sStatus]') IS NOT NULL DROP TABLE [#ds2.1.testSchema.sStatus]
        CREATE TABLE [#ds2.1.testSchema.sStatus] WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
        AS SELECT AA.*
        FROM (SELECT 
              [AssetKey] = src.[AssetKey],
              [AssetSource] = src.[AssetSource], [AssetNumber] = src.[AssetNumber], 
              [StatusCode] = src.[StatusCode], [StatusAttribute] = src.[StatusAttribute]    
            FROM [#ds2.0.testSchema.sStatus] src 
            EXCEPT SELECT
                [AssetKey], [AssetSource], [AssetNumber], [StatusCode], [StatusAttribute]
            FROM [testSchema].[Status]
            WHERE @scdDate BETWEEN scdStartDate AND scdEndDate
        ) AA 
        INNER JOIN [testSchema].[Status] UPD ON AA.AssetKey = UPD.AssetKey AND AA.StatusCode = UPD.StatusCode

        PRINT '-- update inactive records'
        UPDATE [testSchema].[Status]
        SET
          scdCurrentFlag = 'N',
          scdEndDate = @scdDate,
          scdDeletedFlag = 'N',
          dwSessionNo = @sessionNo,
          dwPackageName = @packageName,
          dwUpdateDate = @now
        FROM [#ds2.1.testSchema.sStatus] AA
        WHERE AA.[AssetKey] = [testSchema].[Status].[AssetKey] AND AA.[StatusCode] = [testSchema].[Status].[StatusCode]

        PRINT '-- update deleted records'
        UPDATE [testSchema].[Status]
        SET
          scdCurrentFlag = 'N',
          scdEndDate = DATEADD(s, -1, CAST(@scdDate as DATETIME)),
          -- scdEndDate = @scdDate,
          scdDeletedFlag = 'Y',
          dwSessionNo = @sessionNo,
          dwPackageName = @packageName,
          dwUpdateDate = @now
        WHERE NOT EXISTS (
            SELECT 1 FROM [#ds2.0.testSchema.sStatus] src
            WHERE src.[AssetKey] = [testSchema].[Status].[AssetKey] AND src.[StatusCode] = [testSchema].[Status].[StatusCode]
            )
        AND StatusKey != -1 AND scdEndDate = '9999-12-31 00:00:00.000'

        PRINT '-- load new records'
        INSERT INTO [testSchema].[Status] ( 
            [AssetKey], [AssetSource], [AssetNumber], [StatusCode], [StatusAttribute], 
            
            scdCurrentFlag, scdStartDate, scdEndDate, scdVersion, scdDeletedFlag, 
            dwSessionNo, dwPackageName, dwUpdateDate)
        SELECT
            [AssetKey] = src.[AssetKey],
            [AssetSource] = src.[AssetSource],
            [AssetNumber] = src.[AssetNumber],
            [StatusCode] = src.[StatusCode],
            [StatusAttribute] = src.[StatusAttribute],
            scdCurrentFlag = 'Y',
            scdStartDate = @scdDate,
            -- #custom-code: scdStartDate based on src.<startDate>
            -- scdStartDate = CASE WHEN vers.[GUID] IS NULL AND src.<startDate> < @scdDate
            --   THEN src.<startDate> ELSE @scdDate END,
            scdEndDate = '9999-12-31 00:00:00.000',
            scdVersion = CASE WHEN vers.[AssetKey] IS NULL THEN 1 ELSE vers.scdVersion + 1 END,
            scdDeletedFlag = 'N',
            dwSessionNo = @sessionNo,
            dwPackageName = @packageName,
            dwUpdateDate = @now
        FROM [#ds2.0.testSchema.sStatus] src
        LEFT OUTER JOIN ( --get last version
          SELECT [AssetKey], [StatusCode], MAX(scdVersion) as scdVersion
          FROM [testSchema].[Status] 
          GROUP BY [AssetKey], [StatusCode]
        ) AS vers
        ON vers.[AssetKey] = src.[AssetKey] AND vers.[StatusCode] = src.[StatusCode]
        LEFT JOIN [testSchema].[Status] ds 
        ON src.[AssetKey] = ds.[AssetKey] AND src.[StatusCode] = ds.[StatusCode]
        AND ds.scdCurrentFlag='Y'
        WHERE ds.[AssetKey] IS NULL


        -- update scdEndDate     
        IF OBJECT_ID('tempdb..[#ds2.2.testSchema.sStatus]') IS NOT NULL DROP TABLE [#ds2.2.testSchema.sStatus]
        CREATE TABLE [#ds2.2.testSchema.sStatus] WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
        AS SELECT [AssetKey], [StatusCode], 
          scdVersion, 
          scdEndDate = DATEADD(s, -1, LEAD(scdStartDate) OVER (PARTITION BY [AssetKey], [StatusCode] ORDER BY scdVersion)) 
        FROM [testSchema].[Status]

        UPDATE [testSchema].[Status]
        SET
          scdEndDate = ISNULL(DD.scdEndDate, 
            CASE WHEN [testSchema].[Status].scdCurrentFlag='Y' THEN '9999-12-31 00:00:00.000' ELSE [testSchema].[Status].scdEndDate END),
          dwSessionNo = @sessionNo,
          dwPackageName = @packageName,
          dwUpdateDate = @now
        FROM [#ds2.2.testSchema.sStatus] DD 
        WHERE DD.[AssetKey] = [testSchema].[Status].[AssetKey] AND DD.[StatusCode] = [testSchema].[Status].[StatusCode]
          AND DD.scdVersion = [testSchema].[Status].scdVersion 


        -- ************ UnitTests ****************                

        DECLARE @checkTotalQuantity varchar(250)
        DECLARE @checkQuantityKey varchar(250)
        DECLARE @checkQuantityPK varchar(250)
        DECLARE @checkQuantityLimit varchar(250)
        DECLARE @checkNullPK varchar(250)
        DECLARE @checkDeletedRows varchar(250)
        DECLARE @expectCountMin bigInt, @expectCountMax bigInt
        EXEC etl.getRowLimit @objectCode, @expectCountMin OUT, @expectCountMax OUT
        ;WITH 
            Expect as (SELECT expectCount = count(*) FROM [testSchema].[sStatus]),
            Actual as (SELECT 
                actualCount = SUM(CASE 
                        WHEN [StatusKey] = -1 THEN 0 
                        -- WHEN rowChangeType = 'D' THEN 0 
                        ELSE 1 END),
                actualCountKey = SUM(CASE WHEN [StatusKey] = -1 THEN 1 ELSE 0 END),
                actualCountPK = COUNT(DISTINCT CASE 
                        WHEN [StatusKey] = -1 THEN null 
                        -- WHEN rowChangeType = 'D' THEN null 
                        ELSE CONCAT_WS('~',[AssetKey], [StatusCode], '') END),
                actualNullPK = SUM( CASE WHEN ([AssetKey] is null) OR ([StatusCode] is null) THEN 1 ELSE 0 END )
                -- actualCountDeleted = SUM(CASE 
                --         WHEN rowChangeType = 'D' THEN 1
                --         ELSE 0 END)
                FROM [testSchema].[Status]
                WHERE scdCurrentFlag = 'Y')
        SELECT 
            @checkTotalQuantity = test.assertEqualsInt('checkTotalQuantity (source row quantity = destination)', expectCount, actualCount),
            @checkQuantityKey = test.assertEqualsInt('checkQuantityKey (destination dimKey = 1)', 1, actualCountKey),
            @checkQuantityPK = test.assertEqualsInt('checkQuantityPK (source row quantity = destination PK)', expectCount, actualCountPK),
            -- @checkDeletedRows = test.assertEqualsInt('checkDeletedRows (deleted rows = 0)', 0, actualCountDeleted),
            @checkQuantityLimit = test.assertTrue(CONCAT('checkQuantityLimit (between ', @expectCountMin, ' - ', @expectCountMax, ' rows)'), 
                CASE WHEN actualCount BETWEEN @expectCountMin and @expectCountMax THEN 'TRUE' ELSE 'FALSE' END),
            @checkNullPK = test.assertEqualsInt('checkNullPK (PK has NULL values)', 0, actualNullPK),
            @rowsRead = expectCount, @rowsWritten = actualCount
        FROM Actual CROSS JOIN Expect

        -- Info
        PRINT @checkTotalQuantity
        PRINT @checkQuantityKey
        PRINT @checkQuantityPK
        PRINT @checkQuantityLimit
        
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkTotalQuantity
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkQuantityKey
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkQuantityPK
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkQuantityLimit

        -- Warnings
        -- SET @checkDeletedRows = REPLACE( @checkDeletedRows, 'TEST FAILED', 'WARNING!')
        -- PRINT @checkDeletedRows
        -- EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkDeletedRows
        
        -- Raise Errors
        IF (@checkTotalQuantity like 'TEST FAILED%') RAISERROR (@checkTotalQuantity, 16, 1)
        IF (@checkQuantityPK like 'TEST FAILED%') RAISERROR (@checkQuantityPK, 16, 1)
        IF (@checkQuantityLimit like 'TEST FAILED%') RAISERROR (@checkQuantityLimit, 16, 1)
        IF (@checkNullPK like 'TEST FAILED%') RAISERROR (@checkNullPK, 16, 1)
        
        
        -- ************ SCD2 UnitTests ****************         

        DECLARE @checkCurrentFlag varchar(250)
        DECLARE @checkStartDate varchar(250)
        DECLARE @checkOrphanRows varchar(250)

        ;WITH 
        S as (SELECT id = CONCAT_WS('~',[AssetKey], [StatusCode], '' ), 
            scdStartDate, scdEndDate, scdCurrentFlag 
            FROM [testSchema].[Status]),
        TESTS AS (
            SELECT curS.id, 
                chk_currentFlag = CASE WHEN ISNULL(chkCurFlag.cnt_currentFlag, 0) > 1 THEN 1 ELSE 0 END,
                cnt_orphan_rows = CASE WHEN (chkCurFlag.cnt_currentFlag = 0 AND cnt_currentPeriod = 0) THEN 1 ELSE 0 END, 
                chk_start_date = CASE WHEN ISNULL( DATEADD(s, 1, prevS.scdEndDate), curS.scdStartDate) = curS.scdStartDate THEN 0 ELSE 1 END
            FROM S curS 
            LEFT JOIN S prevS 
                ON prevS.id = curS.id AND (curS.scdStartDate-0.01 BETWEEN prevS.scdStartDate AND prevS.scdEndDate)
            LEFT JOIN (SELECT id, 
                SUM(CASE WHEN (scdCurrentFlag = 'Y') THEN 1 ELSE 0 END) cnt_currentFlag,
                SUM(CASE WHEN (@scdDate BETWEEN scdStartDate AND scdEndDate) THEN 1 ELSE 0 END) cnt_currentPeriod
                FROM S GROUP BY id) chkCurFlag 
                ON curS.id = chkCurFlag.id 
            )  
        -- DEBUG: select all failed records
        -- SELECT * FROM TESTS WHERE chk_currentFlag != 0 OR cnt_orphan_rows != 0 OR chk_start_date != 0     
        SELECT 
            @checkCurrentFlag = test.assertEqualsInt('checkCurrentFlag (0/1 rows per each PK)', 0, sum(chk_currentFlag)),
            @checkStartDate = test.assertEqualsInt('checkStartDate (previous end date = current start date)', 0, sum(chk_start_date)),
            @checkOrphanRows = test.assertEqualsInt('checkOrphanRows (cnt_currentFlag = 0 & cnt_currentPeriod = 0)', 0, sum(cnt_orphan_rows))
        FROM TESTS

        -- Info
        PRINT @checkCurrentFlag
        PRINT @checkStartDate
        
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkCurrentFlag
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkStartDate
        
        -- Warnings
        SET @checkOrphanRows = REPLACE( @checkOrphanRows, 'TEST FAILED', 'WARNING!')
        PRINT @checkOrphanRows
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @checkOrphanRows

        -- Raise Errors
        IF (@checkCurrentFlag like 'TEST FAILED%') RAISERROR (@checkCurrentFlag, 16, 1)
        IF (@checkStartDate like 'TEST FAILED%') RAISERROR (@checkStartDate, 16, 1)
    
    
        -- ************ Finalize transformation here ****************                
        IF OBJECT_ID('tempdb..[#ds2.1.testSchema.sStatus]') IS NOT NULL DROP TABLE [#ds2.1.testSchema.sStatus]
        IF OBJECT_ID('tempdb..[#ds2.2.testSchema.sStatus]') IS NOT NULL DROP TABLE [#ds2.2.testSchema.sStatus]
        


        -- ************ ETL code ends here ****************                

        SET @sessionStatus = 'Succeeded'
        SET @message = etl.setLogMessage('Dimension', 'Transformation complete', REPLACE(REPLACE('"output":{"rowsRead":{0}, "rowsWritten":{1}}','{0}', @rowsRead),'{1}', @rowsWritten), @sessionStatus)
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @message

        -- change object status to @sessionStatus
        EXEC [etl].[setSessionStatus] @sessionNo, @packageName, @objectCode, @status = @sessionStatus, @countRows = @rowsWritten
    END 
    ELSE BEGIN -- else skip this step
        SET @message = etl.setLogMessage('Dimension', 'Skip this step', NULL, @sessionStatus)
        EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @message
    END
END TRY
BEGIN CATCH
    -- change object status to 'Failed'
    SET @sessionStatus = 'Failed'
    SET @message = etl.setLogMessage('Dimension', 'Execution failed', 
                [etl].[setLogErrorMessage] ( ERROR_NUMBER(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_MESSAGE() ),
                @sessionStatus)
    EXEC [etl].[addSessionLog] @sessionNo, @packageName, @objectCode, @procName, @message

    EXEC [etl].[setSessionStatus] @sessionNo, @packageName, @objectCode, @status = @sessionStatus, @countRows = NULL
    RAISERROR (@message, 16, 1)
END CATCH
