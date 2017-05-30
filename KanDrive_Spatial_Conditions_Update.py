#!/usr/bin/env python
# -*- coding: utf-8 -*-
# KanDrive_Spatial_Conditions_Update.py
# Created by Dirktall04 on 2015-10-07
# Reuses portions of the CDRS_Update script by KyleG.
# Modified by Dirktall04 on 2015-10-22
# Aggregate table changes made by Dirktall04 on 2016-08-30

# Change this so that both of the transfer functions
# that currently exist write to an in-memory table
# and then that in-memory table's rows get written
# to the target output table all at once.

print "Script starting."

import os
import sys
import datetime

kanRoadRCRSRoads = r"D:\kandrive\harvesters\scheduled-tasks\kanroad@krprod.sde\KANROAD.RCRS_ROADS"
kanDriveSpatialConditions = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.Conditions"

sdeCDRS = r"D:\kandrive\harvesters\scheduled-tasks\KRPublic.sde\KANROAD.CDRS_ALERT_ROUTE"
pythonLogTable = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.pythonLogging"
metadataTempFolder = r"D:\kandrive\harvesters\scheduled-tasks\metatemp\Conditions"
aggregateTable = r"in_memory\aggregateTable"

print "Starting imports from dt_logging."

try:
	from KDOT_Imports.dt_logging import scriptSuccess
except:
	print "Failed to import scriptSuccess"
	
try:
	from KDOT_Imports.dt_logging import scriptFailure
except:
	print "Failed to import scriptFailure"
	
try:
	from KDOT_Imports.dt_logging import ScriptStatusLogging
except:
	print "Failed to import from KDOT_Imports.dt_logging"
	scriptSuccess = ""
	scriptFailure = ""
	def ScriptStatusLogging(taskName = 'Unavailable', taskTarget = 'Unknown',
                        completionStatus = scriptFailure, taskStartDateTime = datetime.datetime.now(), 
                        taskEndDateTime = datetime.datetime.now(), completionMessage = 'Unexpected Error.',
						tableForLogs = pythonLogTable):
		print "ScriptStatusLogging import failed."

print "Trying to import arcpy functions."
		
#import arcpy functions used in this script
from arcpy import (ClearWorkspaceCache_management, DefineProjection_management,
				Delete_management, Describe, env, Exists, MetadataImporter_conversion,
				FeatureClassToFeatureClass_conversion, 
				TruncateTable_management, XSLTransform_conversion)
from arcpy.da import (InsertCursor as daInsertCursor, SearchCursor as daSearchCursor, UpdateCursor as daUpdateCursor)  # @UnresolvedImport

print "Completed import of arcpy functions."

env.overwriteOutput = True
in_memory = "in_memory"
lambertCC = "PROJCS['NAD_83_Kansas_Lambert_Conformal_Conic_Meters',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-98.0],PARAMETER['standard_parallel_1',38.0],PARAMETER['standard_parallel_2',39.0],PARAMETER['scale_factor',1.0],PARAMETER['latitude_of_origin',38.5],UNIT['Meter',1.0]]"


def RemoveGpHistory_fc(out_xml_dir):
	remove_gp_history_xslt = r"D:\kandrive\harvesters\scheduled-tasks\metadataremoval\removeGeoprocessingHistory.xslt"
	print "Trying to remove out_xml_dir/metadtaTempFolder..."
	if Exists(out_xml_dir):
		Delete_management(out_xml_dir)
	else:
		pass
	os.mkdir(out_xml_dir)
	env.workspace = out_xml_dir
	ClearWorkspaceCache_management()
	
	try:
		print "Starting xml conversion."
		name_xml = "RCRS_LAM.xml"
		#Process: XSLT Transformation
		XSLTransform_conversion(kanDriveSpatialConditions, remove_gp_history_xslt, name_xml, "")
		print("Completed xml conversion on %s") % (kanDriveSpatialConditions)
		# Process: Metadata Importer
		MetadataImporter_conversion(name_xml, kanDriveSpatialConditions)
	except:
		print("Could not complete xml conversion on %s") % (kanDriveSpatialConditions)
		endTime = datetime.datetime.now()
		ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'kandrive_spatial.DBO.Conditions',
			scriptFailure, startTime, endTime, "Could not complete xml conversion on " + kanDriveSpatialConditions,
			pythonLogTable)
		
		# Reraise the error to stop execution and prevent a success message
		# from being inserted into the table.
		raise


def prepareAggregateTable():
    env.workspace = in_memory
    
    FeatureClassToFeatureClass_conversion(kanDriveSpatialConditions,"in_memory","aggregateTable")
    rowDeletionCursor = daUpdateCursor(aggregateTable, '*')
    
    for rowToDelete in rowDeletionCursor:
        rowDeletionCursor.deleteRow()
    
    try:
        del rowDeletionCursor
    except:
        pass


def transferFeaturesToAggregateTable():
	env.workspace = in_memory
	featuresToTransfer = list()
	try:
		# Create an in_memory feature class which to hold the features from
		# the Oracle table.
		FeatureClassToFeatureClass_conversion(kanRoadRCRSRoads,"in_memory","RCRS")
		
		# Then, define a projection on it, since the original Oracle table
		# is lacking the proper information.
		DefineProjection_management("in_memory\RCRS", lambertCC)
		
		###############################################################################################################
		# Maintainability information:
		# If you need to add another field to transfer between the two, just add it to
        # kanRoadRCRSRoads and kanDriveSpatialConditions
		# tables and give it the same name in both.
		###############################################################################################################
		
		# searchCursorFields go to r"in_memory\RCRS". (Input table)(Indirect)
		descObject = Describe(r"in_memory\RCRS")
		searchCursorFields = [field.name for field in descObject.fields if 
							field.name != descObject.OIDFieldName and field.name != "Shape" and
							field.name != "ID1"]
		searchCursorFields.append('SHAPE@')
		
		# Make the insertCursor use the same fields as the searchCursor.
		insertCursorFields = searchCursorFields
		
		print "OIDFieldname = " + descObject.OIDFieldName
		
		print "fieldNames to be used in the searchCursor (and insertCursor):"
		for fieldName in searchCursorFields:
			print fieldName		
		
		RCRS_SearchCursor = daSearchCursor(r"in_memory\RCRS", searchCursorFields)
		
		for RCRS_CursorItem in RCRS_SearchCursor:
			featureItem = list(RCRS_CursorItem)
			featuresToTransfer.append(featureItem)
		
		RCRS_InsertCursor = daInsertCursor(aggregateTable, insertCursorFields)
		
		for RCRS_Feature in featuresToTransfer:
			insertOID = RCRS_InsertCursor.insertRow(RCRS_Feature)
			#print "Inserted a row with the OID of: " + str(insertOID)
		
	
	except:
		print "An error occurred."
		errorItem = sys.exc_info()[1]
		errorStatement = str(errorItem.args[0])
		print errorStatement
		
		if len(errorStatement) > 253:
			errorStatement = errorStatement[0:253]
		else:
			pass
		endTime = datetime.datetime.now()
		ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'aggregateTable',
			scriptFailure, startTime, endTime, errorStatement, pythonLogTable)
			
		try:
			del errorItem
		except:
			pass
		
		# Reraise the error to stop execution and prevent a success message
		# from being inserted into the table.
		raise
		
	finally:
		try:
			del RCRS_SearchCursor
		except:
			pass
		try:
			del RCRS_InsertCursor
		except:
			pass


def transferConstructionWeatherToAggregateTable():
    env.workspace = in_memory
    featuresToTransfer = list()
    try:
        print str(datetime.datetime.now()) + ' copying the oracle table to memory'

        # Create an in_memory feature class which to hold the features from
        # the Oracle table.

        FeatureClassToFeatureClass_conversion(sdeCDRS, "in_memory", "CDRS_Weather", "#")

        # Then, define a projection on it, since the original Oracle table
        # is lacking the proper information.
        DefineProjection_management("in_memory\CDRS_Weather", lambertCC)

        ###############################################################################################################
        # Maintainability information:
        # ToDo:
        # If you need to add another field to transfer between the two, just add it to the searchCursorFields and the
        # insertCursorFields lists and make sure that it is in the same position in the list order for both of
        # them.
        # Besides 'LoadDate', the order does not matter, so long as each field name in the
        # searchCursorFields has a counterpart in the insertCursorFields and vice versa.
        # 'LoadDate' should always be last for the insertCursorFields as it is appended to each row after all
        # of the other items from the searchCursorFields.
        ###############################################################################################################

        # searchCursorFields go to "in_memory\CDRS". (Input table)
        # Removed 'GEOMETRY', then replaced functionality with 'SHAPE@'.
        # Also removed 'OBJECTID', 'RPT_BY_NAME', 'RPT_BY_PHONE',
        # 'RPT_BY_EMAIL', 'CONTACT_NAME', 'CONTACT_PHONE', 'CONTACT_EMAIL',
        # 'BEG_LATITUDE', 'BEG_LONGITUDE', 'BEG_REF_POST', 'END_REF_POST',
        # 'END_LATITUDE', 'END_LONGITUDE', 'INTERNAL_COMMENT',
        # 'WIDTH_RESTRICTION',	'VERT_RESTRICTION', 'WEIGHT_RESTRICTION',
        # 'SPEED_RESTRICTION', 'PUBLIC_VIEW', 'DCAM_COMMENT', 'DCAM_DATE',
        # 'DISPLAY_MAP', 'LINE_COLOR', 'CDRS_ALERT_ROUTE_ID', 'ALERT_ID',
        # 'ALERT_DATE', 'COMP_DATE', 'BEG_COUNTY_NAME',
        # 'END_LRS_KEY', 'END_LRS_ROUTE',	'END_COUNTY_NAME',
        # 'BEG_LRS_DIREC_TXT', 'END_LRS_DIREC_TXT', 'TIME_DELAY_TXT',
        # 'CDRS_WZ_DETAIL_ID', 'CDRS_FHWA_ID', 'CONST_PROJ_NUM',
        # 'PUBLIC_COMMENT', 'OFFICE_NAME', 'NEW_NOTIFICATION',
        # 'EMAIL_REMINDER', 'NOTIFICATION_SENT', 'LOC_ENTRY_TYPE',
        # 'ALERT_HYPERLINK', 'GIS_VIEW',
        # 'END_COUNTY_NUMBER', 'ALERT_TYPE_TXT', 'ALERT_STATUS', 
        # 'CLOSED_MSG_SENT', 'ALERT_INSERT_DT', 'FEA_CLOSED'
        # Public Comment is where to get the output text data from.
        ######################################################################
        searchCursorFields = [
        'SHAPE@', 'ADMIN_OWNER', 'BEG_LRS_KEY', 'ALERT_DIREC_TXT',
        'ALERT_DESC_TXT', 'BEG_COUNTY_NUMBER', 'BEG_STATE_LOGMILE',
        'END_STATE_LOGMILE', 'LENGTH', 'DISTRICT', 'AREA', 'ROUTE',
        'BEG_LRS_ROUTE', 'BEG_DESC', 'END_DESC',
        'SITE_CR', 'LAST_UPDATE',  'START_DATE', 'EXPIRE_DATE']

        # insertCursorFields go to kandrive_spatial.DBO.Conditions. (Output table)
        # Removed 'GEOMETRY' and 'GEOMETRY.STLength()', then replaced their
        # functionality with 'SHAPE@'.
        # Also removed 'OBJECTID'
        ######################################################################
        insertCursorFields = [
        'SHAPE@', 'ADMIN_OWNER', 'LRS_KEY', 'LANE_DESCRIPTION',
        'ROAD_COND_TEXT', 'COUNTY_NO', 'BEG_STATE_LOGMILE',
        'END_STATE_LOGMILE', 'SEG_LENGTH_MILES', 'SNOW_DIST', 'SNOW_AREA',
        'RD_SUFX_SUBC', 'ROUTE_ID', 'BEG_DESC', 'END_DESC', 'SITE_CR',
        'LAST_UPDATE', ### End of directly transfered columns. Next are set columns.
        'STALE_MSG', 'UNIQUE_ID', 'RCRS_ROADS_ID', 'SNOW_SUBAREA',
        'ROAD_CONDITION', 'RE_OPENING', 'CLOSED_MSG_SENT',
        'CLOSED_FROM', 'CLOSED_TO', 'ACCOMMODATION', 
        'USER_ID', 'CLOSED_COND', 'MSG_CODE',
        'LINE_COLOR', 'MSG_SENT', 'SNOW_ICE_CATEGORY', 'RCRSP_VIEW',
        ## End of set columns. Next are calculated columns
        'SEGMENT_ID', 'LRS_PREFIX', 'LRS_SUFFIX', 'LRS_SUBCLASS',
        'RT_NUMBER', 
        'BEG_POINT_DESC', 'END_POINT_DESC', 'RCRSP_DIST', 
        'SNICE_BEGIN', 'SNICE_END', 'REPORT_TIME', 'ROAD_COND_TIME']
        # 46 Total with 29 following LAST_UPDATE

        weatherTypesString = "('Flooding', 'Fog', 'Blowing Dust/Smoke', 'High Winds')" ## 'Wind Damage' changed to 'High Winds' 2016-08-09
        whereClause = """ALERT_STATUS = 2 AND FEA_CLOSED = 1 AND ALERT_TYPE_TXT = 'Road Closing' AND ALERT_DESC_TXT IN """ + weatherTypesString

        cdrsSearchCursor = daSearchCursor(r"in_memory\CDRS_Weather", searchCursorFields, whereClause)

        for cdrsCursorItem in cdrsSearchCursor:
            featureItem = list(cdrsCursorItem)
            ## Copied columns
            copiedFeatureItem = featureItem[:-2]
            lrsKey = featureItem[2]
            startDateCDRS = featureItem[-2]
            startDateStr = startDateCDRS.strftime('%m/%d/%y')
            expDateCDRS = featureItem[-1]
            expDateStr = expDateCDRS.strftime('%m/%d/%y')
            ## Set Columns
            copiedFeatureItem.append(0) ## STALE_MSG
            copiedFeatureItem.append(0) ## UNIQUE_ID
            copiedFeatureItem.append(9999) ## RCRS_ROADS_ID
            copiedFeatureItem.append(9) ## SNOW_SUBAREA
            copiedFeatureItem.append(11) ## ROAD_CONDITION should always == 11, for closed.
            copiedFeatureItem.append(None) ## RE_OPENING
            copiedFeatureItem.append(None) ## CLOSED_MSG_SENT
            copiedFeatureItem.append(None) ## CLOSED_FROM
            copiedFeatureItem.append(None) ## CLOSED_TO
            copiedFeatureItem.append(None) ## ACCOMMODATION
            copiedFeatureItem.append(100) ## USER_ID
            copiedFeatureItem.append(2) ## CLOSED_COND
            copiedFeatureItem.append('0000') ## MSG_CODE
            copiedFeatureItem.append(255) ## LINE_COLOR
            copiedFeatureItem.append(0) ## MSG_SENT
            copiedFeatureItem.append(3) ## SNOW_ICE_CATEGORY
            copiedFeatureItem.append(3) ## RCRSP_VIEW
            ## Calculated Columns
            copiedFeatureItem[4] = 'Closed - ' + copiedFeatureItem[4] ## ROAD_COND_TEXT
            copiedFeatureItem.append(copiedFeatureItem[12]) ## SEGMENT_ID = ROUTE_ID
            copiedFeatureItem.append(lrsKey[3:4]) ## LRS_PREFIX
            copiedFeatureItem.append(lrsKey[9:10]) ## LRS_SUFFIX
            copiedFeatureItem.append(lrsKey[-1:]) ## LRS_SUBCLASS
            rtNumberToFormat = lrsKey[6:9]
            rtNumberFormatted = int(rtNumberToFormat)
            copiedFeatureItem.append(rtNumberFormatted) ## RT_NUMBER
            # Only get the first 60 chars on the next two columns.
            copiedFeatureItem.append(copiedFeatureItem[13][:60]) ## BEG_POINT_DESC
            copiedFeatureItem.append(copiedFeatureItem[14][:60]) ## END_POINT_DESC
            copiedFeatureItem.append(copiedFeatureItem[9]) ## RCRSP_DIST
            copiedFeatureItem.append(startDateStr + ' 12:04 AM') ## SNICE_BEGIN
            copiedFeatureItem.append(expDateStr + ' 11:54 PM') ## SNICE_END
            copiedFeatureItem.append(startDateStr + ' 12:04 AM') ## REPORT_TIME
            copiedFeatureItem.append(startDateStr + ' 12:04 AM') ## ROAD_COND_TIME

            featuresToTransfer.append(copiedFeatureItem)
		
        if len(featuresToTransfer) == 0:
            print("There aren't any weather conditions to transfer\n" +
                "from the construction table to the conditions table.")
        
        RCRS_InsertCursor = daInsertCursor(aggregateTable, insertCursorFields)

        for RCRS_Feature in featuresToTransfer:
            insertOID = RCRS_InsertCursor.insertRow(RCRS_Feature)
            print "Inserted a row with the OID of: " + str(insertOID)

    except:
        print "An error occurred."
        errorItem = sys.exc_info()[1]
        errorStatement = str(errorItem.args[0])
        print errorStatement

        if len(errorStatement) > 253:
            errorStatement = errorStatement[0:253]
        else:
            pass
        endTime = datetime.datetime.now()
        ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'aggregateTable',
            scriptFailure, startTime, endTime, errorItem.args[0], pythonLogTable)
        
        try:
            del errorItem
        except:
            pass
        
        # Reraise the error to stop execution and prevent a success message
        # from being inserted into the table when the script completes.
        raise
        
    finally:
        try:
            del cdrsSearchCursor
        except:
            pass
        try:
            del RCRS_InsertCursor
        except:
            pass


def writeAggregateTableToKanDrive():
    try:
        #truncating CDRS segments in KanDrive Spatial
        print str(datetime.datetime.now()) + " truncating RCRS segments in KanDrive Spatial."
        TruncateTable_management(kanDriveSpatialConditions)
        
        featuresToTransfer = list()
        
        # searchCursorFields go to r"in_memory\RCRS". (Input table)(Indirect)
        descObject = Describe(r"in_memory\RCRS")
        searchCursorFields = [field.name for field in descObject.fields if 
                            field.name != descObject.OIDFieldName and field.name != "Shape" and
                            field.name != "ID1"]
        searchCursorFields.append('SHAPE@')
        
        RCRS_SearchCursor = daSearchCursor(aggregateTable, searchCursorFields)
		
        for RCRS_CursorItem in RCRS_SearchCursor:
			featureItem = list(RCRS_CursorItem)
			featuresToTransfer.append(featureItem)
        
        # Make the insertCursor use the same fields as the searchCursor.
        insertCursorFields = searchCursorFields
        
        RCRS_InsertCursor = daInsertCursor(kanDriveSpatialConditions, insertCursorFields)
            
        for RCRS_Feature in featuresToTransfer:
            insertOID = RCRS_InsertCursor.insertRow(RCRS_Feature)
            print "Inserted a row with the OID of: " + str(insertOID)
		
    except:
        print "An error occurred."
        errorItem = sys.exc_info()[1]
        errorStatement = str(errorItem.args[0])
        print errorStatement
        
        if len(errorStatement) > 253:
            errorStatement = errorStatement[0:253]
        else:
            pass
        endTime = datetime.datetime.now()
        ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'kandrive_spatial.DBO.Conditions',
            scriptFailure, startTime, endTime, errorItem.args[0], pythonLogTable)
            
        try:
            del errorItem
        except:
            pass


def manageLogLength():
    logTableDesc = Describe(pythonLogTable)
    logLengthCheckCursor = daSearchCursor(pythonLogTable, logTableDesc.OIDFieldName)

    shouldTruncate = False

    for logItem in logLengthCheckCursor:
        if int(logItem[0]) > 5000:
            shouldTruncate = True
        else:
            pass

    if shouldTruncate == True:
        print "Log table size is too big."
        print "Truncating log table."
        TruncateTable_management(pythonLogTable)
    else:
        pass


if __name__ == "__main__":
    startTime = datetime.datetime.now()
    print str(startTime) + " starting script"
    RemoveGpHistory_fc(metadataTempFolder)
    manageLogLength()
    prepareAggregateTable()
    transferFeaturesToAggregateTable()
    transferConstructionWeatherToAggregateTable()
    writeAggregateTableToKanDrive()
    endTime = datetime.datetime.now()
    runTime = endTime - startTime
    print str(endTime) + " script completed in " + str(runTime)
    ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'kandrive_spatial.DBO.Conditions',
        scriptSuccess, startTime, endTime, 'Completed successfully.', pythonLogTable)
	

else:
	print "KanDrive_Spatial_Conditions_Update script imported."

'''
def transferFeatures():
	env.workspace = in_memory
	featuresToTransfer = list()
	try:
		# Create an in_memory feature class which to hold the features from
		# the Oracle table.
		FeatureClassToFeatureClass_conversion(kanRoadRCRSRoads,"in_memory","RCRS")
		
		# Then, define a projection on it, since the original Oracle table
		# is lacking the proper information.
		DefineProjection_management("in_memory\RCRS", lambertCC)
		
		#truncating CDRS segments in KanDrive Spatial
		print str(datetime.datetime.now()) + " truncating RCRS segments in KanDrive Spatial."
		TruncateTable_management(kanDriveSpatialConditions)

		
		###############################################################################################################
		# Maintainability information:
		# If you need to add another field to transfer between the two, just add it to both of the
		# tables and give it the same name in both.
		###############################################################################################################
		
		# searchCursorFields go to r"in_memory\RCRS". (Input table)(Indirect)
		descObject = Describe(r"in_memory\RCRS")
		searchCursorFields = [field.name for field in descObject.fields if 
							field.name != descObject.OIDFieldName and field.name != "Shape" and
							field.name != "ID1"]
		searchCursorFields.append('SHAPE@')
		
		# Make the insertCursor use the same fields as the searchCursor.
		insertCursorFields = searchCursorFields
		
		print "OIDFieldname = " + descObject.OIDFieldName
		
		print "fieldNames to be used in the searchCursor (and insertCursor):"
		for fieldName in searchCursorFields:
			print fieldName		
		
		RCRS_SearchCursor = daSearchCursor(r"in_memory\RCRS", searchCursorFields)
		
		for RCRS_CursorItem in RCRS_SearchCursor:
			featureItem = list(RCRS_CursorItem)
			featuresToTransfer.append(featureItem)
		
		RCRS_InsertCursor = daInsertCursor(kanDriveSpatialConditions, insertCursorFields)
		
		for RCRS_Feature in featuresToTransfer:
			insertOID = RCRS_InsertCursor.insertRow(RCRS_Feature)
			print "Inserted a row with the OID of: " + str(insertOID)
		
	
	except:
		print "An error occurred."
		errorItem = sys.exc_info()[1]
		errorStatement = str(errorItem.args[0])
		print errorStatement
		
		if len(errorStatement) > 253:
			errorStatement = errorStatement[0:253]
		else:
			pass
		endTime = datetime.datetime.now()
		ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'kandrive_spatial.DBO.Conditions',
			scriptFailure, startTime, endTime, errorStatement, pythonLogTable)
			
		try:
			del errorItem
		except:
			pass
		
		# Reraise the error to stop execution and prevent a success message
		# from being inserted into the table.
		raise
		
	finally:
		try:
			del RCRS_SearchCursor
		except:
			pass
		try:
			del RCRS_InsertCursor
		except:
			pass


def transferConstructionWeather():
    env.workspace = in_memory
    featuresToTransfer = list()
    try:
		print str(datetime.datetime.now()) + ' copying the oracle table to memory'

		# Create an in_memory feature class which to hold the features from
		# the Oracle table.

		FeatureClassToFeatureClass_conversion(sdeCDRS, "in_memory", "CDRS_Weather", "#")

		# Then, define a projection on it, since the original Oracle table
		# is lacking the proper information.
		DefineProjection_management("in_memory\CDRS_Weather", lambertCC)
		
		###############################################################################################################
		# Maintainability information:
		# ToDo:
		# If you need to add another field to transfer between the two, just add it to the searchCursorFields and the
		# insertCursorFields lists and make sure that it is in the same position in the list order for both of
		# them.
		# Besides 'LoadDate', the order does not matter, so long as each field name in the
		# searchCursorFields has a counterpart in the insertCursorFields and vice versa.
		# 'LoadDate' should always be last for the insertCursorFields as it is appended to each row after all
		# of the other items from the searchCursorFields.
		###############################################################################################################

		# searchCursorFields go to "in_memory\CDRS". (Input table)
		# Removed 'GEOMETRY', then replaced functionality with 'SHAPE@'.
		# Also removed 'OBJECTID', 'RPT_BY_NAME', 'RPT_BY_PHONE',
		# 'RPT_BY_EMAIL', 'CONTACT_NAME', 'CONTACT_PHONE', 'CONTACT_EMAIL',
		# 'BEG_LATITUDE', 'BEG_LONGITUDE', 'BEG_REF_POST', 'END_REF_POST',
		# 'END_LATITUDE', 'END_LONGITUDE', 'INTERNAL_COMMENT',
		# 'WIDTH_RESTRICTION',	'VERT_RESTRICTION', 'WEIGHT_RESTRICTION',
		# 'SPEED_RESTRICTION', 'PUBLIC_VIEW', 'DCAM_COMMENT', 'DCAM_DATE',
		# 'DISPLAY_MAP', 'LINE_COLOR', 'CDRS_ALERT_ROUTE_ID', 'ALERT_ID',
		# 'ALERT_DATE', 'COMP_DATE', 'BEG_COUNTY_NAME',
		# 'END_LRS_KEY', 'END_LRS_ROUTE',	'END_COUNTY_NAME',
		# 'BEG_LRS_DIREC_TXT', 'END_LRS_DIREC_TXT', 'TIME_DELAY_TXT',
		# 'CDRS_WZ_DETAIL_ID', 'CDRS_FHWA_ID', 'CONST_PROJ_NUM',
		# 'PUBLIC_COMMENT', 'OFFICE_NAME', 'NEW_NOTIFICATION',
		# 'EMAIL_REMINDER', 'NOTIFICATION_SENT', 'LOC_ENTRY_TYPE',
		# 'ALERT_HYPERLINK', 'GIS_VIEW',
		# 'END_COUNTY_NUMBER', 'ALERT_TYPE_TXT', 'ALERT_STATUS', 
		# 'CLOSED_MSG_SENT', 'ALERT_INSERT_DT', 'FEA_CLOSED'
		# Public Comment is where to get the output text data from.
		######################################################################
		searchCursorFields = [
		'SHAPE@', 'ADMIN_OWNER', 'BEG_LRS_KEY', 'ALERT_DIREC_TXT',
		'ALERT_DESC_TXT', 'BEG_COUNTY_NUMBER', 'BEG_STATE_LOGMILE',
		'END_STATE_LOGMILE', 'LENGTH', 'DISTRICT', 'AREA', 'ROUTE',
		'BEG_LRS_ROUTE', 'BEG_DESC', 'END_DESC',
		'SITE_CR', 'LAST_UPDATE',  'START_DATE', 'EXPIRE_DATE']
		
		# insertCursorFields go to kandrive_spatial.DBO.Conditions. (Output table)
		# Removed 'GEOMETRY' and 'GEOMETRY.STLength()', then replaced their
		# functionality with 'SHAPE@'.
		# Also removed 'OBJECTID'
		######################################################################
		insertCursorFields = [
		'SHAPE@', 'ADMIN_OWNER', 'LRS_KEY', 'LANE_DESCRIPTION',
		'ROAD_COND_TEXT', 'COUNTY_NO', 'BEG_STATE_LOGMILE',
		'END_STATE_LOGMILE', 'SEG_LENGTH_MILES', 'SNOW_DIST', 'SNOW_AREA',
		'RD_SUFX_SUBC', 'ROUTE_ID', 'BEG_DESC', 'END_DESC', 'SITE_CR',
		'LAST_UPDATE', ### End of directly transfered columns. Next are set columns.
		'STALE_MSG', 'UNIQUE_ID', 'RCRS_ROADS_ID', 'SNOW_SUBAREA',
		'ROAD_CONDITION', 'RE_OPENING', 'CLOSED_MSG_SENT',
		'CLOSED_FROM', 'CLOSED_TO', 'ACCOMMODATION', 
		'USER_ID', 'CLOSED_COND', 'MSG_CODE',
		'LINE_COLOR', 'MSG_SENT', 'SNOW_ICE_CATEGORY', 'RCRSP_VIEW',
		## End of set columns. Next are calculated columns
		'SEGMENT_ID', 'LRS_PREFIX', 'LRS_SUFFIX', 'LRS_SUBCLASS',
		'RT_NUMBER', 
		'BEG_POINT_DESC', 'END_POINT_DESC', 'RCRSP_DIST', 
		'SNICE_BEGIN', 'SNICE_END', 'REPORT_TIME', 'ROAD_COND_TIME']
		# 46 Total with 29 following LAST_UPDATE
		
		weatherTypesString = "('Flooding', 'Fog', 'Blowing Dust/Smoke', 'High Winds')" ## 'Wind Damage' changed to 'High Winds' 2016-08-09
		whereClause = """ALERT_STATUS = 2 AND FEA_CLOSED = 1 AND ALERT_TYPE_TXT = 'Road Closing' AND ALERT_DESC_TXT IN """ + weatherTypesString
		
		cdrsSearchCursor = daSearchCursor(r"in_memory\CDRS_Weather", searchCursorFields, whereClause)
		
		for cdrsCursorItem in cdrsSearchCursor:
			featureItem = list(cdrsCursorItem)
			## Copied columns
			copiedFeatureItem = featureItem[:-2]
			lrsKey = featureItem[2]
			startDateCDRS = featureItem[-2]
			startDateStr = startDateCDRS.strftime('%m/%d/%y')
			expDateCDRS = featureItem[-1]
			expDateStr = expDateCDRS.strftime('%m/%d/%y')
			## Set Columns
			copiedFeatureItem.append(0) ## STALE_MSG
			copiedFeatureItem.append(0) ## UNIQUE_ID
			copiedFeatureItem.append(9999) ## RCRS_ROADS_ID
			copiedFeatureItem.append(9) ## SNOW_SUBAREA
			copiedFeatureItem.append(11) ## ROAD_CONDITION should always == 11, for closed.
			copiedFeatureItem.append(None) ## RE_OPENING
			copiedFeatureItem.append(None) ## CLOSED_MSG_SENT
			copiedFeatureItem.append(None) ## CLOSED_FROM
			copiedFeatureItem.append(None) ## CLOSED_TO
			copiedFeatureItem.append(None) ## ACCOMMODATION
			copiedFeatureItem.append(100) ## USER_ID
			copiedFeatureItem.append(2) ## CLOSED_COND
			copiedFeatureItem.append('0000') ## MSG_CODE
			copiedFeatureItem.append(255) ## LINE_COLOR
			copiedFeatureItem.append(0) ## MSG_SENT
			copiedFeatureItem.append(3) ## SNOW_ICE_CATEGORY
			copiedFeatureItem.append(3) ## RCRSP_VIEW
			## Calculated Columns
			copiedFeatureItem[4] = 'Closed - ' + copiedFeatureItem[4] ## ROAD_COND_TEXT
			copiedFeatureItem.append(copiedFeatureItem[12]) ## SEGMENT_ID = ROUTE_ID
			copiedFeatureItem.append(lrsKey[3:4]) ## LRS_PREFIX
			copiedFeatureItem.append(lrsKey[9:10]) ## LRS_SUFFIX
			copiedFeatureItem.append(lrsKey[-1:]) ## LRS_SUBCLASS
			rtNumberToFormat = lrsKey[6:9]
			rtNumberFormatted = int(rtNumberToFormat)
			copiedFeatureItem.append(rtNumberFormatted) ## RT_NUMBER
			# Only get the first 60 chars on the next two columns.
			copiedFeatureItem.append(copiedFeatureItem[13][:60]) ## BEG_POINT_DESC
			copiedFeatureItem.append(copiedFeatureItem[14][:60]) ## END_POINT_DESC
			copiedFeatureItem.append(copiedFeatureItem[9]) ## RCRSP_DIST
			copiedFeatureItem.append(startDateStr + ' 12:04 AM') ## SNICE_BEGIN
			copiedFeatureItem.append(expDateStr + ' 11:54 PM') ## SNICE_END
			copiedFeatureItem.append(startDateStr + ' 12:04 AM') ## REPORT_TIME
			copiedFeatureItem.append(startDateStr + ' 12:04 AM') ## ROAD_COND_TIME

			featuresToTransfer.append(copiedFeatureItem)
		
		RCRS_InsertCursor = daInsertCursor(kanDriveSpatialConditions, insertCursorFields)
		
		for RCRS_Feature in featuresToTransfer:
			insertOID = RCRS_InsertCursor.insertRow(RCRS_Feature)
			print "Inserted a row with the OID of: " + str(insertOID)
		
    except:
        print "An error occurred."
        errorItem = sys.exc_info()[1]
        errorStatement = str(errorItem.args[0])
        print errorStatement
        
        if len(errorStatement) > 253:
            errorStatement = errorStatement[0:253]
        else:
            pass
        endTime = datetime.datetime.now()
        ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'kandrive_spatial.DBO.Conditions',
            scriptFailure, startTime, endTime, errorItem.args[0], pythonLogTable)
            
        try:
            del errorItem
        except:
            pass
        
        # Reraise the error to stop execution and prevent a success message
        # from being inserted into the table when the script completes.
        raise
        
    finally:
        try:
            del cdrsSearchCursor
        except:
            pass
        try:
            del RCRS_InsertCursor
        except:
            pass
'''