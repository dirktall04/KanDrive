USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* Get rid of the function if it exists */
BEGIN TRY
  IF OBJECT_ID('dbo.Custom_Next_RowID') IS NOT NULL
    DROP FUNCTION Custom_Next_RowID;
END TRY
BEGIN CATCH
  PRINT 'Function did not exist.';
END CATCH

GO

USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* Setup the next_rowid_custom function */
CREATE FUNCTION [dbo].[Custom_Next_RowID](@owner nvarchar(128), @table nvarchar(128))
RETURNS bigint
AS
BEGIN
  DECLARE @RC int
  DECLARE @rowid int

  EXECUTE @RC = [dbo].[next_rowid]
     @owner
    ,@table
    ,@rowid OUTPUT

  RETURN @RC
END
GO