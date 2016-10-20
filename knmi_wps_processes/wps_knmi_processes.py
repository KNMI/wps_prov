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

def generateContent(netcdf_w):

    ''' return content1 dictionary used in all outputs '''

    content1 = {}
    for k in netcdf_w.ncattrs():
        v = netcdf_w.getncattr(k)
        if k not in ["bundle","lineage","bundle2","lineage2"]:
            #logging.info( str(k+"="+v) )
            '''
            This must be revisited, carries only a quick fix. 
            '''

            if ("DODS" in k) or ("adaguc" in k) :
                logging.info("no need: "+str(k))
            else:
                content1[  str(k).replace(".","_") ] = str(v)
    try:    
        for k, v in netcdf_w.variables.iteritems():   
            # print "var: "+str(k) #.replace(".","_")
            if k not in ["knmi_provenance"]:
                #content1["variable_"+str(k)] = str(v.short_name)
                for x in v.ncattrs():
                    content1["variable_"+str(k)+"_"+x] = v.getncattr(x)
                
                # print v.shape
                try:
                    content1["variable_"+str(k)+"_shape"] = v.shape
                except Exception, e:
                    pass
                
                # print v.size
                try:
                    content1["variable_"+str(k)+"_size"]  = v.size
                except Exception, e:
                    pass

                # # print v.ndim
                # content1["variable_"+str(k)+"_ndim"]  = str(v.ndim)

    except Exception, e:
        logging.error(str(e))       

    return content1 





class KnmiClipcValidationDescriptor( KnmiWebProcessDescriptor ):

    ''' validation wps, not used, left as example for none output wps '''

    # override with validation process
    def process_execute_function(self , inputs, callback, fileOutPath):

        callback(33)

        logging.error(inputs) 

        variables = [   "title",
                        "summary",
                        "tracking_id",
                        "keywords",
                        "experiment",
                        "institute_id",
                        "contact_mail",
                        "date_creat",
                        "time_coverage_start",
                        "time_coverage_end",
                        "geospatial_lat_min",
                        "geospatial_lat_max",
                        "geospatial_lon_min",
                        "geospatial_lon_max",
                        "geospatial_lat_resolution"]

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
        self.structure["abstract"] = "KNMI WPS Process: CLIPC netcdf metadata validator. Checks netCDF global nc attributes for relevant metadata fields. The result is printed locally. This function is deprecated by the DRS tool."
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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
                                                 fileOutPath+inputs['netcdf_target'].getValue() 

                                                 )

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
        self.structure["abstract"] = "KNMI WPS Process: Simple Copy. The process allows for a direct copy of a local netcdf or of an OpenDAP enabled netcdf link. The copy is available in the local scratch directory and can be uploaded in the basket for use with other WPS services." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/CWD/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/CWD_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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
    # http://<host>/impactportal/WPS?service=WPS&request=getcapabilities
    # http://<host>/impactportal/WPS?service=WPS&request=execute&identifier=knmi_weight&version=1.0.0&storeexecuteresponse=true&netcdf_source=COPY1.nc&weight=1.2&netcdf_target=X1.nc&variable=vDTR&tags=dre


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
            logging.error (content1)
            logging.error (netcdf_w)
            raise e

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):

        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_weight"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "Copy with weight netcdf" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC weight. This is an advanced copy function, the output is a netcdf with a floating point variable of the dataset selected. The data can be multiplied by a constant, represented by the weight field. Further more a normalisation can be applied, three normalisation methods are provided. These are no normalisation, min max linear normalisation and std deviation normalisation."
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
                           
#                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/tudo/tier3/forest_arcgis-10-4-0_IRPUD_JRC-LUISA-Landuse_10yr_20100101-20501231.nc",
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
#                            "default"    : "vDTR" ,
                            "default"    : "forest" ,
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
        capath= os.environ.get('CAPATH')
        callback(20)
        # validator old                 
        #(metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )
        try:

            bbox =  inputs['bbox'].getValue()[0]+","+inputs['bbox'].getValue()[1]+","+inputs['bbox'].getValue()[2]+","+inputs['bbox'].getValue()[3]

            target = fileOutPath+inputs['netcdf_target'].getValue()


            #adagucservice = "https://pc150396.knmi.nl:9443/impactportal/adagucserver?"
            adagucservice = "https://climate4impact.eu/impactportal/adagucserver?";#Default adagucservice. Now checking SERVICE_ADAGUCSERVER env
            
            #Impactportal sets the right one in the env
            if( os.environ.get('SERVICE_ADAGUCSERVER') != None ):
              adagucservice = os.environ.get('SERVICE_ADAGUCSERVER')
            else:
              callback(23 , info="no environment var=SERVICE_ADAGUCSERVER")

            serviceLink =  adagucservice+'source='+inputs['netcdf_source'].getValue()

            callback(25 , info=serviceLink)

            netcdf_w = processlib.getWCS( serviceLink , 
                                bbox , 
                                inputs['time'].getValue(), 
                                target,
                                inputs['width'].getValue(), 
                                inputs['height'].getValue(),
                                certfile,
                                capath)
  

            #content of prov... move...
            netcdf_w = netCDF4.Dataset( target , 'a')

            processlib.createKnmiProvVar(netcdf_w)

            content1 = generateContent(netcdf_w)
                 
        except Exception, e:
            content1 = {"copy_error": str(e) } 
            logging.info (netcdf_w)
            logging.info (content1)
            raise

        return content1 , source1, netcdf_w



    def __init__( self ):

        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_wcs"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "WCS Process" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: WCS Wrapper.  A WCS service is used to extract a smaller subset of the opendap netcdf source. The 2D subset is defined by a bounding box, single time element and a height/width resolution of the data. The WCS service is also able to extract the median of a ensemble data set for simple visualisation."
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
                            #"default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/knmi/RCM/EUR-44/BC/tasmin/tr_icclim-4-2-3_KNMI_ens-multiModel_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-44_SMHI-DBS43_EOBS10_bcref-1981-2010_yr_20060101-20991231.nc",
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
                            "default"    : "2006-07-01T00:00:00Z" ,
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
            logging.error (content1)

            raise e

        callback(99)    
        
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_combine"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "CLIPC Combine" # = 'SimpleIndices',
        self.structure["abstract"] ="KNMI WPS Process: Simple combination of two inputs into a single netCDF. The simple combine requires to equally sized data sets for simple comparison. Currently available functions are "+str(op.keys())
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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
                            "values"     : ["add","subtract","multiply","divide", "equal", "less" , "greater","nutsextraction"]
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
        callback(21)
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

        callback(48)    
        
        try:
            target = fileOutPath+knmiprocess.inputs['netcdf_target'].getValue()

            #content of prov... move...
            netcdf_w = netCDF4.Dataset( target , 'a')


            processlib.createKnmiProvVar(netcdf_w)

            callback(58)

            content1 = generateContent(netcdf_w)    

        except Exception, e:
            content1 = {"copy_error": str(e) } 

            traceback.print_exc(file='/tmp/wpsserver.log') #sys.stderr)

            raise e
        
        callback(68)    

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_advanced_combine"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "CLIPC Advanced Combine"  # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC Advanced combine two inputs into a single netCDF. The combine function provides a visual exploration tool for dataset pairs. Any two datasets can be resized via wcs to a single time instance and compared using numpy arithmetic and normalisation tools. The two datasets being compared are normalised and can be weighted to provide improved visual comparison.  The combine function is primarily an exploration tool, with a high level of uncertainty."
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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
        self.structure["abstract"] = 'KNMI WPS Process: CLIPC Normalise Advanced. The normalisation method allows for linear normalisation based on a central value with boundaries.  The central value will represent a midpoint, and the maximum of the dataset. The output boundaries are the min and max values.  All other values are not given a value (NaN).' 
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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
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


class KnmiNormaliseLinearDescriptor( KnmiWebProcessDescriptor ):

    # KnmiNormaliseAdvancedDescriptor

    # override with validation process
    def process_execute_function(self , inputs, callback,fileOutPath):
        

        callback(10)

        logging.error(inputs) 

        content1 = {}

        source1 = [inputs['netcdf_source'].getValue()]

        try:
            netcdf_w = processlib.normaliseLinearNetCDF( inputs['netcdf_source'].getValue()     ,
                                                inputs['b'].getValue()            ,
                                                inputs['a'].getValue()            ,
                                                inputs['variable'].getValue()     ,
                                                fileOutPath+inputs['netcdf_target'].getValue() )   


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

        self.structure["identifier"] = "knmi_norm_linear"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "Normalise using a linear equation" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC Normalise Linear. The dataset is transformed using a linear equation. The offset is represented by the a variables, and the rate is the b variable. It is an exploratory tool for visualising impact indicators, leading to a high level of uncertainty."
        self.structure["version"] = "1.0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "norm input: Input netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_MON_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "b" , 
                            "title"      : "norm input: B input.",
                            "type"       : type(float),
                            "default"    : "1.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "a" , 
                            "title"      : "norm input: A input.",
                            "type"       : type(float),
                            "default"    : "0.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "variable" , 
                            "title"      : "norm input: VariableName of netCDF layer." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "norm input: Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "NORM_LINEAR.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "norm input: User Defined Tags CLIPC user tags." ,
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
        ave = inputs['average'].getValue()

        target = fileOutPath+inputs['netcdf_target'].getValue()
        source1 = [sourceA,sourceB]
        
        try:            
            # cd /usr/people/mihajlov/climexp/bin/correlatefield
            #'./bin/correlatefield DATA/cru_ts3.22.1901.2013.pre.dat.nc DATA/nino3.nc mon 1:12 ave 3 DATA/out.nc'

            callback(21)

            dirscript = os.environ['PYWPS_PROCESSES']

            #PYWPS_PROCESSES
            # os.environ['PYWPS_PROCESSES']

            loc = '/usr/people/mihajlov/climexp'
            # script = './climexp/correlatefield '+loc+sourceA+' '+loc+sourceB+' '+freq+' '+ratio+' '+ave+' 3 '+str(target)

            # process = Popen(cmd, stdout=PIPE, stderr=PIPE, env=envhpc, shell=True)
            script = dirscript+'/climexp/correlatefield '+sourceA+' '+sourceB+' '+freq+' '+ratio+' '+ave+' 3 '+str(target)

            callback(22,info=script)

            try:
                process = subprocess.Popen( script  , shell=True).communicate() 
                callback(23,info=str(process))
            except Exception, e:
                callback(23,info="Popen process failed")
                raise e
            
            #,stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE

                       
            #outs, errs = process.communicate()

           
            #callback(24,info="errs: "+errs)

            #process.wait()

            #  /usr/people/mihajlov/climexp/

            callback(25,info=target)

            netcdf_w = netCDF4.Dataset(target,'a')

            ''' metadata appended, inspire worthy '''
            netcdf_w.setncattr(  "title"        , "KNMI Climate Explorer correlate field service output" )
            netcdf_w.setncattr(  "summary"      , "KNMI Climate Explorer correlate field service output "+sourceA+" for "+sourceB )
            netcdf_w.setncattr(  "keywords"     , "climate,correlation,wps_knmi" )
            netcdf_w.setncattr(  "institute_id" , "KNMI" )            
            netcdf_w.setncattr(  "contact"      , "oldenborgh@knmi.nl" )
            netcdf_w.setncattr(  "date_created" , datetime.now().isoformat() )

            # for sinspire...
            #netcdf_w.setncattr(  "time_coverage_start" , " " )
            #netcdf_w.setncattr(  "time_coverage_end"   , " " )
            #netcdf_w.setncattr(  "geospatial_lat_min"  , " " )
            #netcdf_w.setncattr(  "geospatial_lat_max"  , " " )
            #netcdf_w.setncattr(  "geospatial_lon_min"  , " " )
            #netcdf_w.setncattr(  "geospatial_lon_max"  , " " )
            #netcdf_w.setncattr(  "geospatial_lat_resolution" , "1 m" )
            #netcdf_w.setncattr(  "geospatial_lon_resolution" , "1 m" )

            netcdf_w.setncattr(  "knmi_wps" , self.structure["identifier"] )

            callback(24,info="creating knmi prov var")
            processlib.createKnmiProvVar(netcdf_w)
            
            content1 = generateContent(netcdf_w)    

        except Exception, e:
            content1 = {"copy_error": str(e) , "target":target , "process" : process} 
            logging.info(netcdf_w)
            logging.info(content1)
            logging.info(str(e))

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
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/climate_explorer/cru_ts3.22.1901.2013.pre.dat.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_source2" , 
                            "title"      : "Copy input: Input 2 netCDF opendap." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/climate_explorer/nino3.nc" ,
                            "abstract"   : "application/netcdf",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Output netCDF." ,
                            "type"       : type("String"),
                            "default"    : "out.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "frequency" , 
                            "title"      : "Frequency" ,
                            "type"       : type("String"),
                            "default"    : "mon" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "ratio" , 
                            "title"      : "Ratio" ,
                            "type"       : type("String"),
                            "default"    : "1:12" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "average" , 
                            "title"      : "Average" ,
                            "type"       : type("String"),
                            "default"    : "ave" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "provenance_research_knmi",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self
