USE [kandrive_spatial]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* Get rid of the function if it exists */
BEGIN TRY
  IF OBJECT_ID('dbo.GML311PointDetector') IS NOT NULL
    DROP FUNCTION GML311PointDetector;
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

CREATE FUNCTION [dbo].[GML311PointDetector](@xmlInput nvarchar(max))
RETURNS int
AS
BEGIN
  DECLARE @intOutput int;
  DECLARE @pointCounter int;
  DECLARE @pResult int;
  DECLARE @xmlTemp nvarchar(max);
  DECLARE @pMultiCurve bigint;
  DECLARE @testPattern nvarchar(max);
  DECLARE @patternLength bigint;
  SET @xmlTemp = @xmlInput;
  SET @intOutput = 0;
  /* Clear out any dots in web links that may be present, i.e. from
     xmlns="http://www.opengis.net/gml" text. */
  SET @xmlTemp = REPLACE(@xmlTemp, 'www.', '');
  SET @xmlTemp = REPLACE(@xmlTemp, '.com', '');
  SET @xmlTemp = REPLACE(@xmlTemp, '.net', '');
  SET @xmlTemp = REPLACE(@xmlTemp, '.org', '');
  /* Multicurves will have at least 2 points by definition, so only check non-Multicurves */
  SET @pMultiCurve = PATINDEX('%MultiCurve%', @xmlTemp);
  SET @testPattern = '%.%';
  SET @patternLength = LEN(@testPattern) - 2
  /* Have to modify this if using a string that starts or ends in a space. */
  SET @pointCounter = 0;
  IF @pMultiCurve = 0
    BEGIN
	  /* Attempt to remove any junk Z-coordinate values prior to counting. */
	  SET @xmlTemp = REPLACE(@xmlTemp, ' 0.0', '');
	  SET @pResult = PATINDEX(@testPattern, @xmlTemp);
	  WHILE NOT (@pResult = 0 or @pResult IS NULL or @pointCounter >= 20)
	    BEGIN
	    SET @pResult = PATINDEX(@testPattern, @xmlTemp);
	    IF @pResult = 0 or @pResult IS NULL or @pointCounter >= 20
		  BREAK
	    ELSE
		  BEGIN
		    SET @xmlTemp = STUFF(@xmlTemp, @pResult, @patternLength, '');
		    SET @pointCounter = @pointCounter + 1;
		  END
	  /* Tests for more than one pair of x/y coordinates. Even if there is a Z coord
	     that slips by this should still be a valid test. */
	  IF @pointCounter >= 4
	    BEGIN
	      SET @intOutput = 1;
	    END
	  ELSE
	    BEGIN
	      SET @intOutput = 0;
	    END
	  END
    END
  ELSE
    BEGIN
      SET @intOutput = 1;
	END
  RETURN @intOutput;
END
GO

/* Test the function here, since you can't use print statements within it. */
DECLARE @testString nvarchar(max);
SET @testString = '<gml:Curve srsName="SDO:" xmlns:gml="http://www.opengis.net/gml"><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">59195.4062855 -51625.86921576 59300.7675839963 -51434.2585341179 59314.5450301428 -51405.9157797491 59338.3267830789 -51350.7519696091 59363.4641028011 -51285.3773700598 59383.2218266999 -51228.6666486249 59413.0768271384 -51128.0259583507 59425.2217999052 -51079.5742093333 59438.0380671284 -51020.9941890818 59453.758873571 -50932.422886354 59465.3483572247 -50843.1715786225 59471.0239521186 -50773.4333066773 59474.0126726211 -50683.3238267786 59471.5460471192 -50412.2976559619 59464.6460469472 -49654.1822497041 59463.3071701806 -49574.720668956 59457.5702395135 -49234.2671633033 59451.9851700591 -48745.4579744047 59451.8525855441 -48733.8716738153 59449.2816865621 -48508.8233044137 59446.8248994428 -48458.8250098188 59445.3309869308 -48439.3466921049 59439.6799284475 -48365.6378695556 59440.6000383897 -48360.6803135443 59439.3266140457 -48331.9297219811 59440.7395436059 -48327.1573697843 59440.6362294267 -48289.2741001268 59442.5152386453 -48284.6537867777 59443.2380418544 -48265.9894676096 59445.3719966201 -48261.4227948404 59446.1037271904 -48252.3486121769 59445.4830298433 -48238.3450674051 59446.3762596118 -48222.5832411791 59447.51640606 -48202.4637817468 59447.2997455932 -48178.7580724345 59442.3697458895 -48103.9029527549 59431.9014159939 -48034.8826855603 59430.9512586376 -48030.8670187467 59427.6771906657 -48017.0260997065 59415.7976996715 -47966.8139213268 59394.5444259385 -47900.148430281 59379.9838995427 -47862.8598618856 59359.3083288614 -47817.4130967263 59338.7179883314 -47777.5269258532 59313.4791036289 -47734.3780448233 59005.7660340813 -47254.1165263703 58967.1817733385 -47193.4385083612 58944.3415343456 -47157.5190669539 58726.5985145386 -46815.0942660556 58681.7096556457 -46748.7208657692 58639.0329550008 -46693.298002199 58593.2948566421 -46640.272340327 58540.8325250784 -46586.729292783 58488.5793992178 -46540.1882688431 58425.6813210019 -46490.8047853619 58367.9530393443 -46451.3291268995 58307.9702143079 -46415.4432927211 57521.1435345366 -46000.724493351 57462.3449019205 -45969.7280924222 57370.1709606319 -45921.1362961581 </gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>';
PRINT 'Result from pointDetector = ' + CAST(dbo.GML311PointDetector(@testString) as NVARCHAR);

/* More testing... */
BEGIN
  DECLARE @xmlInput nvarchar(max);
  SET @xmlInput = 'gml:Curve srsName="SDO:" xmlns:gml="http://www.opengis.net/gml"><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">59195.4062855 -51625.86921576 0.0 59300.7675839963 1.358742 0.0</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>';
  DECLARE @intOutput int;
  DECLARE @pointCounter int;
  DECLARE @pResult int;
  DECLARE @xmlTemp nvarchar(max);
  DECLARE @pMultiCurve bigint;
  DECLARE @testPattern nvarchar(max);
  DECLARE @patternLength bigint;
  SET @xmlTemp = @xmlInput;
  SET @intOutput = 0;
  /* Clear out any dots in web links that may be present, i.e. from
     xmlns="http://www.opengis.net/gml" text. */
  SET @xmlTemp = REPLACE(@xmlTemp, 'www.', '');
  SET @xmlTemp = REPLACE(@xmlTemp, '.com', '');
  SET @xmlTemp = REPLACE(@xmlTemp, '.net', '');
  SET @xmlTemp = REPLACE(@xmlTemp, '.org', '');
  /* Multicurves will have at least 2 points by definition, so only check non-Multicurves */
  SET @pMultiCurve = PATINDEX('%MultiCurve%', @xmlTemp);
  SET @testPattern = '%.%';
  SET @patternLength = LEN(@testPattern) - 2
  /* Have to modify this if using a string that starts or ends in a space. */
  SET @pointCounter = 0;
  IF @pMultiCurve = 0
    BEGIN
	  /* Attempt to remove any junk Z-coordinate values prior to counting. */
	  SET @xmlTemp = REPLACE(@xmlTemp, ' 0.0', '');
	  SET @pResult = PATINDEX(@testPattern, @xmlTemp);
	  WHILE NOT (@pResult = 0 or @pResult IS NULL or @pointCounter >= 20)
	    BEGIN
	    SET @pResult = PATINDEX(@testPattern, @xmlTemp);
	    IF @pResult = 0 or @pResult IS NULL or @pointCounter >= 20
		  BREAK
	    ELSE
		  BEGIN
		    SET @xmlTemp = STUFF(@xmlTemp, @pResult, @patternLength, '');
		    SET @pointCounter = @pointCounter + 1;
		  END
	  /* Tests for more than one pair of x/y coordinates. Even if there is a Z coord
	     that slips by this should still be a valid test. */
	  IF @pointCounter >= 4
	    BEGIN
	      SET @intOutput = 1;
	    END
	  ELSE
	    BEGIN
	      SET @intOutput = 0;
	    END
	  END
    END
  ELSE
    BEGIN
      SET @intOutput = 1;
	END
  SELECT @intOutput;
END
GO