USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* Get rid of the function if it exists */
BEGIN TRY
  IF OBJECT_ID('dbo.formatOracleGML311Lines') IS NOT NULL
    DROP FUNCTION formatOracleGML311Lines;
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

/* Or, (re)create it as a table valued function. */
CREATE FUNCTION [dbo].[formatOracleGML311Lines](@xmlInput nvarchar(max))
RETURNS nvarchar(max)
AS
BEGIN
  DECLARE @xmlOutput nvarchar(max);
  DECLARE @xmlTemp nvarchar(max);
  DECLARE @xmlTempTesting nvarchar(max);
  DECLARE @pMultiCurve bigint;
  SET @xmlTemp = @xmlInput;
  /* Do the formatting for Curves */
  SET @pMultiCurve = PATINDEX('%MultiCurve%', @xmlTemp);
  IF @pMultiCurve = 0
    BEGIN
      SET @xmlTemp = REPLACE(@xmlTemp, 'gml:', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ':gml', '');
      SET @xmlTemp = REPLACE(@xmlTemp, 'Curve srsName="SDO:"', 'LineString');
      SET @xmlTemp = REPLACE(@xmlTemp, '<segments><LineStringSegment>', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension=""', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="0"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="1"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="2"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="3"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, '</LineStringSegment>', '');
      SET @xmlTemp = REPLACE(@xmlTemp, '</segments>', '');
      SET @xmlTemp = REPLACE(@xmlTemp, '</Curve>', '</LineString>');
      SET @xmlTemp = REPLACE(@xmlTemp, ' </posList>', '</posList>');
	  SET @xmlTemp = REPLACE(@xmlTemp, ' 0.0', '');
    END
  ELSE
    BEGIN
      SET @xmlTemp = REPLACE(@xmlTemp, 'gml:', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ':gml', '');
      SET @xmlTemp = REPLACE(@xmlTemp, 'MultiCurve srsName="SDO:"', 'MultiCurve');
      SET @xmlTemp = REPLACE(@xmlTemp, '<Curve>', '<LineString>');
      SET @xmlTemp = REPLACE(@xmlTemp, 'curveMember', 'curveMembers');
      SET @xmlTemp = REPLACE(@xmlTemp, '<segments><LineStringSegment>', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension=""', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="0"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="1"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="2"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, ' srsDimension="3"', '');
      SET @xmlTemp = REPLACE(@xmlTemp, '</LineStringSegment>', '');
      SET @xmlTemp = REPLACE(@xmlTemp, '</segments>', '');
      SET @xmlTemp = REPLACE(@xmlTemp, '</Curve>', '</LineString>');
      SET @xmlTemp = REPLACE(@xmlTemp, '</curveMembers><curveMembers><LineString><posList', '<LineString><posList');
      SET @xmlTemp = REPLACE(@xmlTemp, ' </posList>', '</posList>');
	  SET @xmlTemp = REPLACE(@xmlTemp, ' 0.0', '');
    END
  SET @xmlOutput = @xmlTemp;
  RETURN @xmlOutput;
END
GO