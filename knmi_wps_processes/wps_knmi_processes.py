import processlib
from wps_knmi import KnmiWebProcessDescriptor
from pywps.Process import Status
import netCDF4
import numpy as np
from datetime import datetime
import os
import sys, traceback
import wps_knmi
import time
import logging

#from clipcombine.clipc_combine_process import clipc_combine_process
# run from run.wps.here.py (this allows the local cgi to be used...)
# author: ANDREJ
# tests provenance with knmi wps.

        # def #logger_info(str1):
        #   with open('/nobackup/users/mihajlov/impactp/tmp/server2.log','a') as f:
        #     f.write(str(str1)+"\n")
        #   f.close()

        ##logger_info("doit! process_execute_function")


def generateContent(netcdf_w):

    ''' return content1 dictionary used in all outputs '''

    content1 = {}
    for k in netcdf_w.ncattrs():
        v = netcdf_w.getncattr(k)
        if k not in ["bundle","lineage","bundle2","lineage2"]:
            #if "DODS" not in k:
            #content1[str(k).replace(".","_")] = str(v)
            #content1[str(k)] = str(v)
            content1[str(k).replace(".","_")] = str(v)
    try:    
        for k, v in netcdf_w.variables.iteritems():   
            # print "var: "+str(k) #.replace(".","_")
            if k not in ["knmi_provenance"]:
                #content1["variable_"+str(k)] = str(v.short_name)
                for x in v.ncattrs():
                    content1["variable_"+str(k)+"_"+x] = str(v.getncattr(x))
                
                # print v.shape
                try:
                    content1["variable_"+str(k)+"_shape"] = str(v.shape)
                except Exception, e:
                    pass
                
                # print v.size
                try:
                    content1["variable_"+str(k)+"_size"]  = str(v.size)
                except Exception, e:
                    pass

                # # print v.ndim
                # content1["variable_"+str(k)+"_ndim"]  = str(v.ndim)

    except Exception, e:
        logging.debug(str(e))       
 
    return content1 





class KnmiClipcValidationDescriptor( KnmiWebProcessDescriptor ):

    ''' validation wps, not used, left as example for none output wps '''

    # override with validation process
    def process_execute_function(self , inputs, callback, fileOutPath):

        callback(33)

        logging.error(inputs) 

        variables = ["title",
                        "summary",
                        "description",
                        "keywords",
                        "driving_experiment",
                        "comment",
                        "institute_id",
                        "in_var_institution",
                        "contact",
                        "contact_mail",
                        "creation_date",
                        "time_coverage_start",
                        "time_coverage_end",
                        "geospatial_lat_min",
                        "geospatial_lat_max",
                        "geospatial_lon_min",
                        "geospatial_lon_max",
                        "resolution"]

        (metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )

        logging.error(content1)
        logging.error( metaTestAnswer )

        if not os.path.exists(fileOutPath):
            os.rmdirs(fileOutPath)

        #prov.content.append(content1)
        callback(44)

        return {"missing": [metaTestAnswer]} , inputs['netcdf'].getValue() , None


    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_clipc_validator"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "CLIPC Validator" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC netcdf metadata validator. Checks netCDF global ncattributes for relevant metadata fields." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf" , 
                            "title"      : "Validator input: netCDF opendap link." ,
                            "type"       : "String",
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "values"     : None, 
                            "abstract"   :"application/netcdf"
                            }  
                            ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }              
                          ]


        self.processExecuteCallback = self.process_execute_function



class KnmiCopyDescriptor( KnmiWebProcessDescriptor ):


    ''' Copy data wps, best example of simple input/output case '''
    # override with validation process
    def process_execute_function(self , inputs, callback , fileOutPath):

        callback(10)

        content1 = {}
        source1  = [inputs['netcdf_source'].getValue()]
        # validator old                 
        
        callback(20)
        #(metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )
        try:
            netcdf_w = processlib.copyNetCDF(    inputs['netcdf_source'].getValue() ,
                                                 fileOutPath+inputs['netcdf_target'].getValue() )

            #content of prov... move...
            # for k in netcdf_w.ncattrs():
            #   v = netcdf_w.getncattr(k)
            #   if k not in ["bundle","lineage"]:
            #     content1[str(k).replace(".","_")] = str(v) 
            content1 = generateContent(netcdf_w)

        except Exception, e:
            callback(70)
            content1 = {"copy_error": str(e) } 
            logging.error (netcdf_w)
            logging.error (content1)

            raise e
        callback(80)

        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_copy"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "Copy netcdf" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC netcdf metadata validator. Checks netCDF global ncattributes for relevant metadata fields." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "Copy input: Input netCDF." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "values"     : None, 
                            "abstract"   :"application/netcdf"                            
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Copy input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "COPY_OUT.nc",
                            "values"     : None,
                            "minOccurs"  : 1,
                            "maxOccurs"  : 1
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]

        self.processExecuteCallback = self.process_execute_function

class KnmiWeightCopyDescriptor( KnmiWebProcessDescriptor ):

    ''' weighted copy and normalisation wps, versitile and used actively '''

    # via terminal
    # http://pc150396.knmi.nl:9080/impactportal/WPS?service=WPS&request=getcapabilities
    # http://pc150396.knmi.nl:9080/impactportal/WPS?service=WPS&request=execute&identifier=knmi_weight&version=1.0.0&storeexecuteresponse=true&netcdf_source=COPY1.nc&weight=1.2&netcdf_target=X1.nc&variable=vDTR&tags=dre


    # override with validation process
    def process_execute_function(self , inputs, callback,fileOutPath):
        
        callback(10)

        content1 = {}
        source1 = [inputs['netcdf_source'].getValue()]


        callback(20)

        try:            
            netcdf_w = processlib.weightNetCDF( inputs['netcdf_source'].getValue()     ,
                                                inputs['weight'].getValue()            ,
                                                inputs['variable'].getValue()          ,
                                                fileOutPath+inputs['netcdf_target'].getValue() )   

            content1 = generateContent(netcdf_w)    

        except Exception, e:
            content1 = {"copy_error": str(e) } 
            logging.error (netcdf_w)
            logging.error (content1)

            raise e

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):

        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_weight"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "Copy with weight netcdf" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC weight." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "Copy input: Input netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "weight" , 
                            "title"      : "Copy input: Weight of netCDF input. [\"normnone\" , \"normminmax\", \"normstndrd\"]" ,
                            "type"       : type("String"),
                            "default"    : "1.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "variable" , 
                            "title"      : "Copy input: VariableName of netCDF layer." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Copy input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "WEIGHT.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self




class KnmiWcsDescriptor( KnmiWebProcessDescriptor ):

    ''' wcs client used insiede wps '''


    # override with validation process
    def process_execute_function(self , inputs, callback,fileOutPath):


        callback(10)

        content1 = {}

        source1 = [inputs['netcdf_source'].getValue()]

        certfile = os.environ['HOME']+'certs/creds.pem'

        callback(20)
        # validator old                 
        #(metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )
        try:

            bbox =  inputs['bbox'].getValue()[0]+","+inputs['bbox'].getValue()[1]+","+inputs['bbox'].getValue()[2]+","+inputs['bbox'].getValue()[3]

            target = fileOutPath+inputs['netcdf_target'].getValue()

#            netcdf_w = processlib.getWCS(  'https://climate4impact.eu/impactportal/adagucserver?source='+inputs['netcdf_source'].getValue(), 
            netcdf_w = processlib.getWCS(  'https://pc150396.knmi.nl:9443/impactportal/adagucserver?source='+inputs['netcdf_source'].getValue(), 
                                bbox , 
                                inputs['time'].getValue(), 
                                target,
                                inputs['width'].getValue(), 
                                inputs['height'].getValue(),
                                certfile )

                   

            #content of prov... move...
            netcdf_w = netCDF4.Dataset( target , 'a')

            processlib.createKnmiProvVar(netcdf_w)

            # for k in netcdf_w.ncattrs():
            #   v = netcdf_w.getncattr(k)
            #   if k not in ["bundle","lineage","bundle2","lineage2"]:
            #     content1[str(k).replace(".","_")] = str(v) 
            content1 = generateContent(netcdf_w)
                 
        except Exception, e:
            content1 = {"copy_error": str(e) } 
            logging.error (netcdf_w)
            logging.error (content1)

            raise e

        return content1 , source1, netcdf_w



    def __init__( self ):

        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_wcs"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "WCS service within wps" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: WCS Wrapper." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"


        # self.bbox  = self.addLiteralInput(identifier = "bbox" ,title = "Bounding box in defined coordinate system",   type="String",minOccurs=4,maxOccurs=4,default="-40,20,60,85")
  
        # self.time1 = self.addLiteralInput(identifier = "time1",title = "Time 1 for netcdf input A",                   type="String",minOccurs=1,maxOccurs=1,default="2010-10-16T00:00:00Z")



        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "Copy input: Input netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            # "default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "bbox" , 
                            "title"      : "Copy input: BBOX of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : "-40,20,60,85" ,
                            "values"     : None,
                            "maxOccurs"  : 4
                            } ,
                            { 
                            "identifier" : "time" , 
                            "title"      : "Copy input: Time of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : "2010-09-16T00:00:00Z" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Copy input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "WCS.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "width" , 
                            "title"      : "Copy input: width of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : '200' ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "height" , 
                            "title"      : "Copy input: height of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : '200' ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self





'''
operations possible 
'''

op = {  "add"       :np.add,
        "subtract"  :np.subtract,
        "multiply"  :np.multiply,
        "divide"    :np.divide ,
        "equal"     :np.equal  ,
        "less"      :np.less ,
        "greater"   :np.greater }

class KnmiCombineDescriptor( KnmiWebProcessDescriptor ):

    ''' basic combine operation of two netcdfs '''

    # override with validation process
    def process_execute_function(self , inputs, callback, fileOutPath):

        callback(22)

        logging.error(inputs) 

        content1 = {}

        source1 = [inputs['netcdf_source1'].getValue() , inputs['netcdf_source2'].getValue()]

        try:
            operation = op[inputs['operation'].getValue()]
        except Exception ,e:
            raise "Error exception in operator"

        callback(33)    
        try:
            netcdf_w = processlib.combineNetCDF( inputs['netcdf_source1'].getValue()      ,
                                                 inputs['variable1'].getValue()           ,
                                                 inputs['netcdf_source2'].getValue()      ,
                                                 inputs['variable2'].getValue()           ,
                                                 fileOutPath+inputs['netcdf_target'].getValue()       ,
                                                 operation )

            #prov.output = netcdf_w
            #print netcdf_w
            callback(44)

            content1 = generateContent(netcdf_w)

        except Exception, e:
            content1 = {"copy_error": str(e) } 
            logging.debug(netcdf_w)
            logging.error (content1)

            raise e

        callback(99)    
        
        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_combine"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "Combine two inputs into a single netCDF." # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC Combine." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source1" , 
                            "title"      : "Combine input: Source 1 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_JAN_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "values"     : None, 
                            "abstract"   :"application/netcdf"
                            } ,
                            { 
                            "identifier" : "variable1" , 
                            "title"      : "Combine input: VariableName 1." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_source2" , 
                            "title"      : "Combine input: Source 2 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            # "default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "values"     : None, 
                            "abstract"   :"application/netcdf"
                            } ,
                            { 
                            "identifier" : "variable2" , 
                            "title"      : "Combine input: VariableName 2." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Combine input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "COMBO.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "operation" , 
                            "title"      : "Combine input: Operation." ,
                            "type"       : type("String"),
                            "default"    : "add",
                            "values"     : ["add","subtract","multiply","divide", "equal", "less" , "greater"]
                            } , 
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Combine input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self





class KnmiAdvancedCombineDescriptor( KnmiWebProcessDescriptor ):

    ''' advanced combine class, uses multiple wps's '''

    # override with validation process
    def process_execute_function(self , inputs, callback, fileOutPath):

        callback(10)

        content1 = {}

        source1 = [inputs['netcdf_source1'].getValue() , inputs['netcdf_source2'].getValue()]


        try:
            operation = op[inputs['operation'].getValue()]
        except Exception ,e:
            raise "Error exception in operator"

        
        #inputs['']

        # fileOutPath
        ''' WCS '''
        try:
            knmiprocess =  wps_knmi.KnmiWpsProcess(KnmiWcsDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath
            knmiprocess.inputs['netcdf_source'].setValue(   {'value' : inputs['netcdf_source1'].getValue() })
            knmiprocess.inputs['bbox'].setValue(            {'value' : inputs['bbox'].getValue() } )
            knmiprocess.inputs['time'].setValue(            {'value' : inputs['time1'].getValue() } )
            knmiprocess.inputs['netcdf_target'].setValue(   {'value' : 'COPY_WCS1.nc'} )
            knmiprocess.inputs['width'].setValue(           {'value' : inputs['width'].getValue() } )
            knmiprocess.inputs['height'].setValue(          {'value' : inputs['height'].getValue() } )
            knmiprocess.inputs['tags'].setValue(            {'value' : inputs['tags'].getValue() } )

            logging.debug("Starting, creating 1 "+ knmiprocess.inputs['netcdf_target'].getValue());      
            knmiprocess.status = self.process.status
            knmiprocess.execute()
            logging.debug("Done, creating 1 "+ knmiprocess.inputs['netcdf_target'].getValue());      

            knmiprocess =  wps_knmi.KnmiWpsProcess(KnmiWcsDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath
            knmiprocess.inputs['netcdf_source'].setValue(   {'value' : inputs['netcdf_source2'].getValue()})
            knmiprocess.inputs['bbox'].setValue(            {'value' : inputs['bbox'].getValue() } )
            knmiprocess.inputs['time'].setValue(            {'value' : inputs['time2'].getValue() } )
            knmiprocess.inputs['netcdf_target'].setValue(   {'value' : 'COPY_WCS2.nc'} )
            knmiprocess.inputs['width'].setValue(           {'value' : inputs['width'].getValue() } )
            knmiprocess.inputs['height'].setValue(          {'value' : inputs['height'].getValue() } )
            knmiprocess.inputs['tags'].setValue(            {'value' : inputs['tags'].getValue() } )

            logging.debug("Starting, creating 2 "+ knmiprocess.inputs['netcdf_target'].getValue());
            knmiprocess.status = self.process.status
            knmiprocess.execute()
            logging.debug("Done, creating 2 "+ knmiprocess.inputs['netcdf_target'].getValue());

        except Exception, e:
            traceback.print_exc(file=sys.stderr)
            raise e


        ''' NORMALISE '''
   
        try:
            knmiprocess =  wps_knmi.KnmiWpsProcess(KnmiWeightCopyDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath

            knmiprocess.inputs['netcdf_source'].setValue(   {'value' : knmiprocess.fileOutPath1+'COPY_WCS1.nc' })
            knmiprocess.inputs['netcdf_target'].setValue(   {'value' : 'COPY_NORM1.nc'} )
            knmiprocess.inputs['weight'].setValue(          {'value' : inputs['norm1'].getValue() } )
            knmiprocess.inputs['variable'].setValue(        {'value' : inputs['variable1'].getValue() } )
            knmiprocess.inputs['tags'].setValue(            {'value' : inputs['tags'].getValue() } )

            knmiprocess.status = self.process.status
            knmiprocess.execute()
            
            knmiprocess =  wps_knmi.KnmiWpsProcess(KnmiWeightCopyDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath

            knmiprocess.inputs['netcdf_source'].setValue(   {'value': knmiprocess.fileOutPath1+'COPY_NORM1.nc'})
            knmiprocess.inputs['netcdf_target'].setValue(   {'value': 'COPY_WEIGHT1.nc'} )
            knmiprocess.inputs['weight'].setValue(          {'value': inputs['weight1'].getValue() } )
            knmiprocess.inputs['variable'].setValue(        {'value': inputs['variable1'].getValue()} )
            knmiprocess.inputs['tags'].setValue(            {'value': inputs['tags'].getValue() } )

            knmiprocess.status = self.process.status
            knmiprocess.execute()

            knmiprocess =  wps_knmi.KnmiWpsProcess(KnmiWeightCopyDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath
            
            knmiprocess.inputs['netcdf_source'].setValue(   {'value': knmiprocess.fileOutPath1+'COPY_WCS2.nc'})
            knmiprocess.inputs['netcdf_target'].setValue(   {'value':'COPY_NORM2.nc'} )
            knmiprocess.inputs['weight'].setValue(          {'value' : inputs['norm2'].getValue() } )
            knmiprocess.inputs['variable'].setValue(        {'value' : inputs['variable2'].getValue() } )
            knmiprocess.inputs['tags'].setValue(            {'value' : inputs['tags'].getValue() } )

            knmiprocess.status = self.process.status
            knmiprocess.execute()
            
            knmiprocess =  wps_knmi.KnmiWpsProcess(KnmiWeightCopyDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath

            knmiprocess.inputs['netcdf_source'].setValue(   {'value':knmiprocess.fileOutPath1+'COPY_NORM2.nc'})
            knmiprocess.inputs['netcdf_target'].setValue(   {'value':'COPY_WEIGHT2.nc'} )
            knmiprocess.inputs['weight'].setValue(          {'value' : inputs['weight2'].getValue() } )
            knmiprocess.inputs['variable'].setValue(        {'value' : inputs['variable2'].getValue() } )
            knmiprocess.inputs['tags'].setValue(            {'value' : inputs['tags'].getValue() } )

            knmiprocess.status = self.process.status
            knmiprocess.execute()

            callback(30)

            ''' COMBINE '''
            knmiprocess = wps_knmi.KnmiWpsProcess(KnmiCombineDescriptor())
            knmiprocess.bundle = self.process.bundle
            # use parent path...
            knmiprocess.fileOutPath1 = fileOutPath

            knmiprocess.inputs['netcdf_source1'].setValue( {'value':knmiprocess.fileOutPath1+'COPY_WEIGHT1.nc'})
            knmiprocess.inputs['netcdf_source2'].setValue( {'value':knmiprocess.fileOutPath1+'COPY_WEIGHT2.nc'})

            knmiprocess.inputs['variable1'].setValue( {'value' : inputs['variable1'].getValue() } )
            knmiprocess.inputs['variable2'].setValue( {'value' : inputs['variable2'].getValue() } )

            knmiprocess.inputs['netcdf_target'].setValue( {'value': inputs['netcdf_target'].getValue()})

            # output # issue...
            #self.process.inputs['netcdf_target'].setValue( {'value': 'COPY_COMBINE_YEAR.nc'})

            knmiprocess.inputs['operation'].setValue( {'value' : inputs['operation'].getValue() } )
            knmiprocess.inputs['tags'].setValue( {'value' : inputs['tags'].getValue() } )
            
            knmiprocess.status = self.process.status
            knmiprocess.execute()


        except Exception, e:
            traceback.print_exc(file=sys.stderr)
            raise e

        callback(40)    
        
        try:
            target = fileOutPath+knmiprocess.inputs['netcdf_target'].getValue()

            #content of prov... move...
            netcdf_w = netCDF4.Dataset( target , 'a')


            processlib.createKnmiProvVar(netcdf_w)

            callback(50)

            #content of prov... move...
            # for k in netcdf_w.ncattrs():
            #   v = netcdf_w.getncattr(k)
            #   if k not in ["bundle","lineage","bundle2","lineage2"]:
            #     content1[str(k).replace(".","_")] = str(v) 
            content1 = generateContent(netcdf_w)    

        except Exception, e:
            content1 = {"copy_error": str(e) } 

            traceback.print_exc(file='/nobackup/users/mihajlov/impactp/tmp/server.log') #sys.stderr)

            raise e
        
        callback(90)    

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_advanced_combine"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "CLIPC Combine two normalised inputs into a single netCDF." # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC Advanced Combine." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source1" , 
                            "title"      : "Combine input: Source 1 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "values"     : None, 
                            "abstract"   :"application/netcdf"
                            } ,
                            { 
                            "identifier" : "variable1" , 
                            "title"      : "Combine input: VariableName 1." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "weight1" , 
                            "title"      : "Combine input: Weight 1 of netCDF input." ,
                            "type"       : type("String"),
                            "default"    : "1.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "norm1" , 
                            "title"      : "Combine input: Norm 1 of netCDF input. [\"normnone\" , \"normminmax\", \"normstndrd\"]" ,
                            "type"       : type("String"),
                            "default"    : "normminmax" ,
                            "values"     : ["normnone" , "normminmax", "normstndrd"]
                            } ,
                            { 
                            "identifier" : "netcdf_source2" , 
                            "title"      : "Combine input: Source 2 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_JAN_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "values"     : None, 
                            "abstract"   :"application/netcdf"
                            } ,
                            { 
                            "identifier" : "variable2" , 
                            "title"      : "Combine input: VariableName 2." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "weight2" , 
                            "title"      : "Combine input: Weight 2 of netCDF input." ,
                            "type"       : type("String"),
                            "default"    : "1.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "norm2" , 
                            "title"      : "Combine input: Norm 2 of netCDF input. [\"normnone\" , \"normminmax\", \"normstndrd\"]" ,
                            "type"       : type("String"),
                            "default"    : "normminmax" ,
                            "values"     : ["normnone" , "normminmax", "normstndrd"]
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Combine input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "COMBO.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "operation" , 
                            "title"      : "Combine input: Operation." ,
                            "type"       : type("String"),
                            "default"    : "add",
                            "values"     : ["add","subtract","multiply","divide", "equal", "less" , "greater"]
                            } , 
                            { 
                            "identifier" : "bbox" , 
                            "title"      : "Copy input: BBOX of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : "-40,20,60,85" ,
                            "values"     : None,
                            "maxOccurs"  : 4
                            } ,
                            { 
                            "identifier" : "width" , 
                            "title"      : "Copy input: width of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : '200' ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "height" , 
                            "title"      : "Copy input: height of WCS slice." ,
                            "type"       : type("String"),
                            "default"    : '200' ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "time1" , 
                            "title"      : "Copy input: Time of WCS slice 1." ,
                            "type"       : type("String"),
                            "default"    : "2010-09-16T00:00:00Z" ,
                            "values"     : None
                            },
                            { 
                            "identifier" : "time2" , 
                            "title"      : "Copy input: Time of WCS slice 2." ,
                            "type"       : type("String"),
                            "default"    : "2010-01-16T00:00:00Z" ,
                            "values"     : None
                            },
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Combine input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self

class KnmiNormaliseAdvancedDescriptor( KnmiWebProcessDescriptor ):

    # KnmiNormaliseAdvancedDescriptor

    # override with validation process
    def process_execute_function(self , inputs, callback,fileOutPath):
        

        callback(10)

        logging.error(inputs) 

        content1 = {}

        source1 = [inputs['netcdf_source'].getValue()]

        try:
            netcdf_w = processlib.normaliseAdvancedNetCDF( inputs['netcdf_source'].getValue()     ,
                                                inputs['min'].getValue()            ,
                                                inputs['max'].getValue()            ,
                                                inputs['centre'].getValue()            ,
                                                inputs['variable'].getValue()          ,
                                                fileOutPath+inputs['netcdf_target'].getValue() )   


            #content of prov... move...
            # for k in netcdf_w.ncattrs():
            #   v = netcdf_w.getncattr(k)
            #   if k not in ["bundle","lineage"]:
            #     content1[str(k).replace(".","_")] = str(v) 

            content1 = generateContent(netcdf_w)      

        except Exception, e:
            content1 = {"copy_error": str(e) } 
            logging.error (netcdf_w)
            logging.error (content1)

            raise e

        return content1 , source1, netcdf_w



    def __init__( self ):

        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_norm_adv"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "Normalise with min, max and centre netcdf" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC Normalise Advanced." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "Copy input: Input netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "min" , 
                            "title"      : "Copy input: a min of netCDF input.",
                            "type"       : type(float),
                            "default"    : "0.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "max" , 
                            "title"      : "Copy input: a max of netCDF input.",
                            "type"       : type(float),
                            "default"    : "2.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "centre" , 
                            "title"      : "Copy input: a centre of netCDF input.",
                            "type"       : type(float),
                            "default"    : "1.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "variable" , 
                            "title"      : "Copy input: VariableName of netCDF layer." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Copy input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "NORMADV.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self


import subprocess

class CorrelatefieldDescriptor( KnmiWebProcessDescriptor ):

    ''' 

    Correlatefield function developed for ClimateExplorer 

    # via terminal
    # cd /usr/people/mihajlov/climexp
    # ./bin/correlatefield DATA/cru_ts3.22.1901.2013.pre.dat.nc DATA/nino3.nc mon 1:12 ave 3 DATA/out.nc

    '''

    # override with validation process
    def process_execute_function(self , inputs, callback,fileOutPath):
        
        callback(10)

        content1 = {}

        sourceA = inputs['netcdf_source1'].getValue()
        sourceB = inputs['netcdf_source2'].getValue()

        ratio = inputs['ratio'].getValue()
        freq = inputs['frequency'].getValue()

        target = fileOutPath+inputs['netcdf_target'].getValue()
        source1 = [sourceA,sourceB]
        
        try:            
            # cd /usr/people/mihajlov/climexp/bin/correlatefield
            #'./bin/correlatefield DATA/cru_ts3.22.1901.2013.pre.dat.nc DATA/nino3.nc mon 1:12 ave 3 DATA/out.nc'

            callback(21)

            #PYWPS_PROCESSES
            # os.environ['PYWPS_PROCESSES']

            loc = '/usr/people/mihajlov/climexp'
            # process = Popen(cmd, stdout=PIPE, stderr=PIPE, env=envhpc, shell=True)
            script = loc+'/bin/correlatefield '+loc+sourceA+' '+loc+sourceB+' '+freq+' '+ratio+' ave '+str(target)

            callback(22,info=script)

            process = subprocess.Popen( script  , shell=True) #,stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE

            

            process.wait()
            #  /usr/people/mihajlov/climexp/

            logging.info( target )

            callback(23,info=target)

            netcdf_w = netCDF4.Dataset(target,'a')

            netcdf_w.setncattr(  "institute_id" , "KNMI" )
            netcdf_w.setncattr(  "knmi_wps" , self.structure["identifier"] )

            processlib.createKnmiProvVar(netcdf_w)
            
            content1 = generateContent(netcdf_w)    

        except Exception, e:
            content1 = {"copy_error": str(e) , "target":target , "process" : process} 
            logging.error (netcdf_w)
            logging.error (content1)

            raise e

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):

        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "wps_climexp_correlatefield"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "climate explorer correlatefield" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI Climate Explorer: correlatefield function" #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source1" , 
                            "title"      : "Copy input: Input 1 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "/DATA/cru_ts3.22.1901.2013.pre.dat.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_source2" , 
                            "title"      : "Copy input: Input 2 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "/DATA/nino3.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : ": Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "out.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "frequency" , 
                            "title"      : "frequency" ,
                            "type"       : type("String"),
                            "default"    : "1.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "ratio" , 
                            "title"      : "ratio" ,
                            "type"       : type("String"),
                            "default"    : "1:12" ,
                            "values"     : None
                            } ,

                            { 
                            "identifier" : "tags" , 
                            "title"      : ": User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self
