USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

drop VIEW [dbo].[Construction_Difference_Table]
GO

USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create view [dbo].[Construction_Difference_Table]
as
select TOP 1000
    s.*
from kandrive_spatial.DBO.Construction_Oracle as s
   left join kandrive_spatial.DBO.CONSTRUCTION as t
    on  s.Cdrs_Wz_Detail_Id = t.Cdrs_Wz_Detail_Id or
	t.Cdrs_Wz_Detail_Id is NULL /* doesn't work when the ids are null in one */
where NOT (s.RouteName = t.RouteName
  and s.BeginMP = t.BeginMP
  and s.EndMP = t.EndMP
  and s.County = t.County
  and s.StartDate = t.StartDate
  and s.CompDate = t.CompDate
  and s.AlertType = t.AlertType
  and s.AlertDescription = t.AlertDescription
  and s.HeightLimit = t.HeightLimit
  and s.WidthLimit = t.WidthLimit
  and s.TimeDelay = t.TimeDelay
  and s.Comments = t.Comments
  and s.ContactName = t.ContactName
  and s.ContactPhone = t.ContactPhone
  and s.ContactEmail = t.ContactEmail
  and s.WebLink = t.WebLink
  and s.AlertStatus = t.AlertStatus
  and s.FeaClosed = t.FeaClosed
  and s.Status = t.Status
  and s.AlertDirectTxt = t.AlertDirectTxt
  and CAST(s.Shape.AsGml() as nvarchar(max)) = CAST(t.Shape.AsGml() as nvarchar(max)) )
  ORDER BY s.OBJECTID, s.Cdrs_Wz_Detail_Id, s.RouteName, s.BeginMP;
GO

select count(*) from Construction_Difference_Table;
select * from Construction_Difference_Table;

/* I want to do an update without a join... but that is not the SQL way. */

MERGE INTO [dbo].[CONSTRUCTION] as T
  USING [dbo].[Construction_Oracle] as S
    ON T.Cdrs_Wz_Detail_Id = S.Cdrs_Wz_Detail_Id
WHEN MATCHED
   AND EXISTS (SELECT S.Cdrs_Wz_Detail_Id, S.RouteName
               EXCEPT
			   SELECT T.Cdrs_Wz_Detail_ID, T.RouteName)
THEN
  UPDATE
    SET	t.OBJECTID		= s.newOBJECTID, -- Need a function here to get the next ObjectID
	t.RouteName			= s.RouteName,
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
	t.Shape				= s.Shape
	FROM (select max([dbo].[CONSTRUCTION].OBJECTID) + 1 as newOBJECTID from [dbo].[CONSTRUCTION])

WHEN NOT MATCHED BY TARGET THEN
  INSERT (RouteName, BeginMP, EndMP, County, StartDate, CompDate, AlertType, AlertDescription,
          HeightLimit, WidthLimit, TimeDelay, Comments, ContactName, ContactPhone, ContactEmail,
		  WebLink, AlertStatus, FeaClosed, Status, AlertDirectTxt, Shape, Cdrs_Wz_Detail_Id)
  VALUES (s.RouteName, s.BeginMP, s.EndMP, s.County, s.StartDate, s.CompDate, s.AlertType,
          s.AlertDescription, s.HeightLimit, s.WidthLimit, s.TimeDelay, s.Comments, s.ContactName,
		  s.ContactPhone, s.ContactEmail, s.WebLink, s.AlertStatus, s.FeaClosed, s.Status,
		  s.AlertDirectTxt, s.Shape, s.Cdrs_Wz_Detail_Id)
WHEN NOT MATCHED BY SOURCE THEN DELETE
WHEN NOT EXISTS
			(SELECT S.Cdrs_Wz_Detail_Id, S.RouteName
               EXCEPT
			   SELECT T.Cdrs_Wz_Detail_ID, T.RouteName)
THEN DELETE;