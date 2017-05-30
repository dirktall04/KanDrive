USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('tempdb.dbo.#TempTable', 'U') IS NOT NULL
  DROP TABLE #TempTable;

SELECT IDENTITY( INT,0,1) AS OBJECTID, v.RouteName, v.BeginMP, v.EndMP, v.County, v.StartDate, v.CompDate,
			v.AlertType, v.AlertDescription, v.HeightLimit, v.WidthLimit, v.TimeDelay, v.Comments,
			v.ContactName, v.ContactPhone, v.ContactEmail, v.WebLink, v.AlertStatus, v.FeaClosed,
			v.Status, v.AlertDirectTxt, v.Shape, v.Cdrs_Wz_Detail_Id,
			v.Shape.STLength() as SHAPE_STLength__, v.Shape.STLength() as SHAPE_STLength_1,
			v.Shape.STLength() as SHAPE_STLength_12
     INTO #TempTable
 FROM [dbo].[Construction_Oracle] as v
 ORDER BY v.RouteName, v.BeginMP, v.EndMP, v.StartDate, v.CompDate

 SELECT * FROM #TempTable
 ORDER BY OBJECTID

DECLARE @userSign nvarchar(128)
DECLARE @userTable nvarchar(128)
SET @userSign = 'dbo'
SET @userTable = 'CONSTRUCTION'
DECLARE @sqlQuery VARCHAR(8000)
DECLARE @queryFinal VARCHAR(8000)

SET @sqlQuery = 'Exec kandrive_spatial.dbo.next_rowid(' + '''' + @userSign + '''' + ',' + '''' + @userTable + '''' + ')'
SET @queryFinal = 'SELECT * FROM OPENQUERY(GDB_DEV,' + '''' + @sqlQuery + '''' + ')' 

MERGE INTO [dbo].[CONSTRUCTION] as t
  USING [dbo].[#TempTable] as s
    ON t.OBJECTID				= s.OBJECTID
		AND t.RouteName			= s.RouteName
		AND t.BeginMP			= s.BeginMP
		AND t.EndMP				= s.EndMP
		AND t.StartDate			= s.StartDate
		AND t.CompDate			= s.CompDate
		AND t.Cdrs_Wz_Detail_Id	= s.Cdrs_Wz_Detail_Id

WHEN MATCHED
THEN
  UPDATE
    SET
	t.OBJECTID			= s.OBJECTID,
	t.BeginMP			= s.BeginMP,
	t.EndMP				= s.EndMP,
	t.County			= s.County,
	t.StartDate			= s.StartDate,
	t.CompDate			= s.CompDate,
	t.AlertType			= s.AlertType,
	t.AlertDescription  = s.AlertDescription,
	t.HeightLimit		= s.HeightLimit,
	t.WidthLimit		= s.WidthLimit,
	t.TimeDelay			= s.TimeDelay,
	t.Comments			= s.Comments,
	t.ContactName		= s.ContactName,
	t.ContactPhone		= s.ContactPhone,
	t.ContactEmail		= s.ContactEmail,
	t.WebLink			= s.WebLink,
	t.AlertStatus		= s.AlertStatus,
	t.FeaClosed			= s.FeaClosed,
	t.Status			= s.Status,
	t.AlertDirectTxt	= s.AlertDirectTxt,
	t.Shape				= geometry::STGeomFromText(s.Shape.STAsText(), 3857),
	t.SHAPE_STLength__	= s.SHAPE_STLength__,
	t.SHAPE_STLength_1	= s.SHAPE_STLength_1,
	t.SHAPE_STLength_12	= s.SHAPE_STLength_12

WHEN NOT MATCHED BY TARGET THEN
  INSERT (OBJECTID, RouteName, BeginMP, EndMP, County, StartDate, CompDate, AlertType, AlertDescription,
			HeightLimit, WidthLimit, TimeDelay, Comments, ContactName, ContactPhone, ContactEmail,
			WebLink, AlertStatus, FeaClosed, Status, AlertDirectTxt, Shape, Cdrs_Wz_Detail_Id,
			SHAPE_STLength__, SHAPE_STLength_1, SHAPE_STLength_12)
  VALUES (s.OBJECTID, s.RouteName, s.BeginMP, s.EndMP, s.County, s.StartDate, s.CompDate, s.AlertType,
			s.AlertDescription, s.HeightLimit, s.WidthLimit, s.TimeDelay, s.Comments, s.ContactName,
			s.ContactPhone, s.ContactEmail, s.WebLink, s.AlertStatus, s.FeaClosed, s.Status,
			s.AlertDirectTxt, geometry::STGeomFromText(s.Shape.STAsText(), 3857), s.Cdrs_Wz_Detail_Id, s.SHAPE_STLength__, s.SHAPE_STLength_1,
			s.SHAPE_STLength_12)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
GO

SELECT * FROM [dbo].[CONSTRUCTION]
ORDER BY OBJECTID