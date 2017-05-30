USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

drop VIEW dbo.Construction_Oracle
GO

USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW dbo.Construction_Oracle
AS
SELECT OBJECTID,
  (SUBSTRING(BEG_LRS_ROUTE, 1, 1) + CAST(CAST(SUBSTRING(BEG_LRS_ROUTE, 4, 3) as INT) as NVARCHAR)) as RouteName,
   BEG_STATE_LOGMILE as BeginMP, END_STATE_LOGMILE as EndMP,
  BEG_COUNTY_NAME as County, ALERT_DATE as StartDate,
  COMP_DATE as CompDate, ALERT_TYPE_TXT as AlertType, ALERT_DESC_TXT as AlertDescription,
  VERT_RESTRICTION as HeightLimit, WIDTH_RESTRICTION as WidthLimit,
  TIME_DELAY_TXT as TimeDelay, PUBLIC_COMMENT as Comments,
  DETOUR_TYPE_TXT as DetourType, DETOUR_DESC as DetourDescription,
  CONTACT_NAME as ContactName, CONTACT_PHONE as ContactPhone, CONTACT_EMAIL as ContactEmail,
  ALERT_HYPERLINK as WebLink, ALERT_STATUS as AlertStatus,
  FEA_CLOSED as FeaClosed,
  (CASE
      WHEN ALERT_STATUS = '2' and FEA_CLOSED <> '1' THEN 'Active'
      WHEN ALERT_STATUS = '2' and FEA_CLOSED = '1' THEN 'Closed'
	  WHEN ALERT_STATUS = '1' THEN 'Planned'
    END) as Status,
  ALERT_DIREC_TXT as AlertDirectTxt, BEG_LONGITUDE as X, BEG_LATITUDE as Y,
  geometry::GeomFromGml(dbo.formatOracleGML311Lines(CAST(geom1 AS NVARCHAR(MAX))), 3857) as Shape,
  (Select convert(datetime, getdate(), 103)) as LoadDate,
  CAST(CDRS_WZ_DETAIL_ID as int) as Cdrs_Wz_Detail_Id

  FROM OpenQuery (KRDEVINT, 'select 
  CDRS_ALERT_ROUTE.OBJECTID,
  CDRS_ALERT_ROUTE.BEG_STATE_LOGMILE,
  CDRS_ALERT_ROUTE.END_STATE_LOGMILE,
  CDRS_ALERT_ROUTE.BEG_COUNTY_NAME,
  CDRS_ALERT_ROUTE.ALERT_DATE,
  CDRS_ALERT_ROUTE.COMP_DATE,
  CDRS_ALERT_ROUTE.ALERT_TYPE_TXT,
  CDRS_ALERT_ROUTE.ALERT_DESC_TXT,
  CDRS_ALERT_ROUTE.VERT_RESTRICTION,
  CDRS_ALERT_ROUTE.WIDTH_RESTRICTION,
  CDRS_ALERT_ROUTE.TIME_DELAY_TXT,
  CDRS_ALERT_ROUTE.PUBLIC_COMMENT,
  CDRS_ALERT_ROUTE.CONTACT_NAME,
  CDRS_ALERT_ROUTE.CONTACT_PHONE,
  CDRS_ALERT_ROUTE.CONTACT_EMAIL,
  CDRS_ALERT_ROUTE.ALERT_HYPERLINK,
  CDRS_ALERT_ROUTE.ALERT_STATUS,
  CDRS_WZ_DETAIL.DETOUR_TYPE_TXT,
  CDRS_WZ_DETAIL.DETOUR_DESC,
  CDRS_ALERT_ROUTE.FEA_CLOSED,
  CDRS_ALERT_ROUTE.ALERT_DIREC_TXT,
  CDRS_ALERT_ROUTE.BEG_LONGITUDE,
  CDRS_ALERT_ROUTE.BEG_LATITUDE,
  SDO_UTIL.TO_GML311GEOMETRY(CDRS_ALERT_ROUTE.geometry)geom1,
  CDRS_ALERT_ROUTE.BEG_LRS_ROUTE,
  CDRS_ALERT_ROUTE.CDRS_WZ_DETAIL_ID
  FROM CDRS_ALERT_ROUTE
  LEFT JOIN CDRS_WZ_DETAIL
  ON CDRS_ALERT_ROUTE.CDRS_WZ_DETAIL_ID = CDRS_WZ_DETAIL.CDRS_WZ_DETAIL_ID
  ORDER BY CDRS_ALERT_ROUTE.CDRS_WZ_DETAIL_ID')

  WHERE (dbo.GML311PointDetector(CAST(geom1 as nvarchar(max))) <> 0)
GO

/* Testing */
select * from Construction_Oracle

/* Testing */
select OBJECTID, Shape.AsGml() from Construction_Oracle

/* More testing */
SELECT *
  FROM OpenQuery (KRDEVINT, 'select
  CDRS_ALERT_ROUTE.OBJECTID,
  CDRS_ALERT_ROUTE.CDRS_WZ_DETAIL_ID,
  CDRS_ALERT_ROUTE.ALERT_STATUS,
  SDO_UTIL.TO_GML311GEOMETRY(CDRS_ALERT_ROUTE.geometry)geom1
  FROM CDRS_ALERT_ROUTE')