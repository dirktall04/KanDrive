#!/usr/bin/env python
# -*- coding: utf-8 -*-
# KanDrive_Spatial_Incidents_Update.py
# Created by Dirktall04 on 2015-11-17
# Reuses portions of the CDRS_Update script by KyleG.

import os
import sys
import datetime

startTime = (datetime.datetime.now()) 
print "Script starting."

###kanDriveSpatialConstruction = r"D:\kandrive\harvesters\scheduled-tasks\kanroad@krprod.sde\KANROAD.RCRS_ROADS"
###kanDriveSpatialIncidents = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.Conditions"

# Set up a config here so that this is more portable between Dev and Prod.
try:
    from config import sdeCDRS, kanDriveSpatialIncidents, pythonLogTable, metadataTempFolder # @UnresolvedImport
except:
    print "Import from config failed."
    print "Setting config variables with static values."
    sdeCDRS = r"D:\kandrive\harvesters\scheduled-tasks\kanroad@krprod.sde\KANROAD.CDRS_ALERT_ROUTE"
    ##kanDriveSpatialConstruction = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.Construction"
    kanDriveSpatialIncidents = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.Incidents"
    pythonLogTable = r"D:\kandrive\harvesters\scheduled-tasks\geo_admin@KanDrive_Spatial_Prod.sde\kandrive_spatial.DBO.pythonLogging"
    metadataTempFolder = r"D:\kandrive\harvesters\scheduled-tasks\metatemp"

print "Starting imports from dt_logging."

try:
    from KDOT_Imports.dt_logging import scriptSuccess # @UnresolvedImport
except:
    print "Failed to import scriptSuccess"

try:
    from KDOT_Imports.dt_logging import scriptFailure # @UnresolvedImport
except:
    print "Failed to import scriptFailure"

try:
    from KDOT_Imports.dt_logging import ScriptStatusLogging # @UnresolvedImport
except:
    print "Failed to import from KDOT_Imports.dt_logging"
    scriptSuccess = ""
    scriptFailure = ""
    def ScriptStatusLogging(taskName = 'KanDrive_Spatial_Incidents_Update', taskTarget = 'kandrive_spatial.DBO.Incidents',
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
    # Change this to only affect the xml file and not the directory.
    # Tried that previously and had some issues with it not deleting, but
    # use the python OS tools and not arcpy for it this time. Might work better.
    if Exists(out_xml_dir):
        Delete_management(out_xml_dir)
    else:
        pass
    os.mkdir(out_xml_dir)
    env.workspace = out_xml_dir
    ClearWorkspaceCache_management()
    
    try:
        print "Starting xml conversion."
        name_xml = "kanDriveSpatialIncidents.xml"
        #Process: XSLT Transformation
        XSLTransform_conversion(kanDriveSpatialIncidents, remove_gp_history_xslt, name_xml, "")
        print("Completed xml conversion on %s") % (kanDriveSpatialIncidents)
        # Process: Metadata Importer
        MetadataImporter_conversion(name_xml, kanDriveSpatialIncidents)
    except:
        print("Could not complete xml conversion on %s") % (kanDriveSpatialIncidents)
        endTime = datetime.datetime.now()
        ScriptStatusLogging('KanDrive_Spatial_Incidents_Update', 'kandrive_spatial.DBO.Incidents',
            scriptFailure, startTime, endTime, "Could not complete xml conversion on " + kanDriveSpatialIncidents,
            pythonLogTable)
        
        # Reraise the error to stop execution and prevent a success message
        # from being inserted into the table.
        raise


def transferFeatures():
    env.workspace = in_memory
    featuresToTransfer = list()
    try:
        print str(datetime.datetime.now()) + ' copying the oracle table to memory'
        
        
        # Create an in_memory feature class which to hold the features from
        # the Oracle table.
        FeatureClassToFeatureClass_conversion(sdeCDRS,"in_memory","CDRS","#","ALERT_STATUS <>  3 AND AlertType = 'Road Incident'")
        
        # Then, define a projection on it, since the original Oracle table
        # is lacking the proper information.
        DefineProjection_management("in_memory\CDRS", lambertCC)

        #truncating CDRS segments in KanDrive Spatial
        print str(datetime.datetime.now()) + " truncating Incident segments in KanDrive Spatial."
        TruncateTable_management(kanDriveSpatialIncidents)


        ###############################################################################################################
        # Maintainability information:
        # If you need to add another field to transfer between the two, just add it to both of the
        # tables and give it the same name in both.
        # If you need to add another field to just one, the list comprehension will automatically
        # exclude it.
        #
        # If you need to add another field to transfer between the two and it has to be named
        # different things in each table, you will need to append the matching name to the searchCursorFields list
        # and insertCursorFields list are generated.
        #
        # I.e. if you add a field named "snowFlakeCount" to the CDRS_ALERT_ROUTE table and a field named
        # "snowFlakeCounter" to the kandrive_spatial.DBO.Incidents table, you would need to append
        # "snowFlakeCount" to the searchCursorFields and "snowFlakeCounter" to the insertCursorFields for
        # them to match up and transfer properly. -- If possible, avoid this by naming them both the same thing.
        #
        # If you're having to do several appends, it may be best to just write out all of the field names for
        # each list in the order that you would like for them to be transfered. This is how the field names
        # are listed in the Kandrive_Construction_Update.py script.
        ###############################################################################################################
        
        CDRS_Desc_Object = Describe(r"in_memory\CDRS")
        CDRS_Desc_Fields = [field.name for field in CDRS_Desc_Object.fields]
        Incidents_Desc_Object = Describe(kanDriveSpatialIncidents)
        Incidents_Desc_Fields = [field.name for field in Incidents_Desc_Object.fields]
        
        # This Python list comprehension creates the intersection of the two *_Fields lists
        # and makes sure that the Shape field and Object ID fields are not directly
        # transfered. -- The 'SHAPE@' token indirectly transfers the geometry instead
        # and the Object ID of the target feature class is automatically calculated
        # by the insert cursor.
        searchCursorFields = [fieldName for fieldName in CDRS_Desc_Fields if 
                              fieldName in Incidents_Desc_Fields and
                              fieldName != CDRS_Desc_Object.OIDFieldName and
                              fieldName != Incidents_Desc_Object.OIDFieldName and
                              fieldName != 'Shape']
        
        searchCursorFields.append('SHAPE@')
        
        # Make the insertCursor use the same fields as the searchCursor.
        insertCursorFields = searchCursorFields
        
        print "OIDFieldnames: " + CDRS_Desc_Object.OIDFieldName + " & " + Incidents_Desc_Object.OIDFieldName + "."
        
        print "fieldNames to be used in the searchCursor (and insertCursor):"
        for fieldName in searchCursorFields:
            print fieldName        
        
        Incidents_Where_Clause = "ALERT_TYPE_TXT = 'Road Incident'"
        
        CDRS_SearchCursor = daSearchCursor(r"in_memory\CDRS", searchCursorFields, Incidents_Where_Clause)
        
        for CDRS_CursorItem in CDRS_SearchCursor:
            featureItem = list(CDRS_CursorItem)
            featuresToTransfer.append(featureItem)
        
        Incidents_InsertCursor = daInsertCursor(kanDriveSpatialIncidents, insertCursorFields)
        
        for CDRS_Feature in featuresToTransfer:
            insertOID = Incidents_InsertCursor.insertRow(CDRS_Feature)
            print "Inserted a row with the OID of: " + str(insertOID)
            
        '''
        print "fieldNames to be used in the searchCursor:"
        for fieldName in searchCursorFields:
            print fieldName        
        
        incidents_Where_Clause = "AlertType = 'Road Incident'"
        
        Construction_SearchCursor = daSearchCursor(sdeCDRS, searchCursorFields, incidents_Where_Clause)
        
        for Construction_CursorItem in Construction_SearchCursor:
            featureItem = list(Construction_CursorItem)
            featuresToTransfer.append(featureItem)
        
        Incidents_InsertCursor = daInsertCursor(kanDriveSpatialIncidents, insertCursorFields)
        
        for Construction_Feature in featuresToTransfer:
            insertOID = Incidents_InsertCursor.insertRow(Construction_Feature)
            print "Inserted a row with the OID of: " + str(insertOID)
    
    '''
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
        ScriptStatusLogging('KanDrive_Spatial_Incidents_Update', 'kandrive_spatial.DBO.Incidents',
            scriptFailure, startTime, endTime, errorItem.args[0], pythonLogTable)
            
        try:
            del errorItem
        except:
            pass
        
        # Reraise the error to stop execution and prevent a success message
        # from being inserted into the table.
        raise
        
    finally:
        try:
            del CDRS_SearchCursor
        except:
            pass
        try:
            del Incidents_InsertCursor
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
    print str(startTime) + " starting script"
    RemoveGpHistory_fc(metadataTempFolder)
    manageLogLength()
    transferFeatures()
    endTime = datetime.datetime.now()
    runTime = endTime - startTime
    print str(endTime) + " script completed in " + str(runTime)
    ScriptStatusLogging('KanDrive_Spatial_Incidents_Update', 'kandrive_spatial.DBO.Incidents',
        scriptSuccess, startTime, endTime, 'Completed successfully.', pythonLogTable)


else:
    print "KanDrive_Spatial_Conditions_Update script imported."