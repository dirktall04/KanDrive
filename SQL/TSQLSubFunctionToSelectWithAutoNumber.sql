USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

SELECT ROW_NUMBER() OVER(Order by RouteName, BeginMP, EndMP, StartDate, CompDate) as OBJECTID
	  ,[RouteName]
      ,[BeginMP]
      ,[EndMP]
      ,[County]
      ,[StartDate]
      ,[CompDate]
      ,[AlertType]
      ,[AlertDescription]
      ,[HeightLimit]
      ,[WidthLimit]
      ,[TimeDelay]
      ,[Comments]
      ,[DetourType]
      ,[DetourDescription]
      ,[ContactName]
      ,[ContactPhone]
      ,[ContactEmail]
      ,[WebLink]
      ,[AlertStatus]
      ,[FeaClosed]
      ,[Status]
      ,[AlertDirectTxt]
      ,[X]
      ,[Y]
      ,[Shape]
      ,[LoadDate]
      ,[Cdrs_Wz_Detail_Id]
  FROM [dbo].[Construction_Oracle]
GO

 SELECT s.Cdrs_Wz_Detail_Id, 
     IDENTITY( INT,1,1) AS IncrementingNumbers
     INTO #temp
 FROM  dbo.Construction_Oracle s
 ORDER BY s.Cdrs_Wz_Detail_Id


 SELECT * FROM #temp
 ORDER BY IncrementingNumbers