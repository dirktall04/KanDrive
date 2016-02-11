#!/usr/bin/env python
# -*- coding: utf-8 -*-
# KanDrive_Spatial_Conditions_Update.py
# Created by Dirktall04 on 2015-10-07
# Reuses portions of the CDRS_Update script by KyleG.
# Modified by Dirktall04 on 2015-10-22

print "Script starting."

import os
import sys
import datetime

kanRoadRCRSRoads = r"D:\kandrive\harvesters\scheduled-tasks\kanroad@krprod.sde\KANROAD.RCRS_ROADS"
kanDriveSpatialConditions = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.Conditions"
pythonLogTable = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.pythonLogging"
metadataTempFolder = r"D:\kandrive\harvesters\scheduled-tasks\metatemp"

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
from arcpy.da import (InsertCursor as daInsertCursor, SearchCursor as daSearchCursor)  # @UnresolvedImport

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
	transferFeatures()
	endTime = datetime.datetime.now()
	runTime = endTime - startTime
	print str(endTime) + " script completed in " + str(runTime)
	ScriptStatusLogging('KanDrive_Spatial_Conditions_Update', 'kandrive_spatial.DBO.Conditions',
					scriptSuccess, startTime, endTime, 'Completed successfully.', pythonLogTable)									
	

else:
	print "KanDrive_Spatial_Conditions_Update script imported."