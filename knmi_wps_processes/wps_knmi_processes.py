import processlib
from wps_knmi import KnmiWebProcessDescriptor
from pywps.Process import Status
from pprint import pprint
import netCDF4
import numpy as np
#from clipcombine.clipc_combine_process import clipc_combine_process
# run from run.wps.here.py (this allows the local cgi to be used...)
# author: ANDREJ
# tests provenance with knmi wps.


# move to other place...

# class KnmiClipcNormalisationWPS(KnmiWebProcessDescriptor):

#     # override this function to allow 
#     def process_function(self ,inputs, callback):
#         # print "process_function" 
#         # #pprint (inputs) 
#         # pprint (inputs) 

#         link_opendap = inputs['netcdf'].getValue()
#         method       = inputs['norm'].getValue()

#         nc_fid = netCDF4.Dataset( link_opendap ,'r')

#         var = nc_fid.variables['vDTR'][:]

#         normalised = clipc_wp8_norm.norm( var , method )

#         nc_fid = netCDF4.Dataset( 'test.nc' ,'w')     


#     def __init__(self):
#         self.structure = {}      
#         self.inputsTuple = []

#         self.structure["identifier"] = "knmi_clipc_norm"   # = 'wps_simple_indice', # only mandatary attribute = same file name
#         self.structure["title"]= "KNMI WPS: CLIPC normalisation" # = 'SimpleIndices',
#         self.structure["abstract"] = "KNMI WPS Process: CLIPC Dataset normalisation tool"
#         self.structure["storeSupported"] = True
#         self.structure["statusSupported"] = True
#         self.structure["grassLocation"] = False
#         self.structure["metadata"] = "METADATA D4P"

#         # input tuple describes addLiteralInput, values
#         self.inputsTuple = [ 
#                             { 
#                             "identifier" : "netcdf" , 
#                             "title"      : "Normaliser input: netCDF opendap link." ,
#                             "type"       : "String",
#                             "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
#                             "values"     : None #"TG","TX","TN","TXx","TXn","TNx"]
#                             },
#                             { 
#                             "identifier" : "norm" , 
#                             "title"      : "Normaliser input: normalisation method." ,
#                             "type"       : "String",
#                             "default"    : "" ,
#                             "values"     : clipc_wp8_norm.nrm.keys()
#                             }                  
#                           ]


#         self.processCallback = self.process_function

class KnmiClipcValidationDescriptor( KnmiWebProcessDescriptor ):


    # override with validation process
    def process_execute_function(self , inputs, callback):

        callback("process_function")

        pprint(inputs) 

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

        pprint(content1)
        pprint( metaTestAnswer )

        #prov.content.append(content1)

        return {"missing": [metaTestAnswer]} , inputs['netcdf'].getValue() , None


    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_clipc_validator"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "KNMI WPS: CLIPC Validator" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC netcdf metadata validator. Checks netCDF global ncattributes for relevant metadata fields." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "0.0"
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
                            "values"     : None #"TG","TX","TN","TXx","TXn","TNx"]
                            }              
                          ]


        self.processExecuteCallback = self.process_execute_function


class KnmiCopyDescriptor( KnmiWebProcessDescriptor ):


    # override with validation process
    def process_execute_function(self , inputs, callback):

        callback("process_execute_function copy")

        #pprint(inputs) 
        content1 = {}

        source1 = [inputs['netcdf_source'].getValue()]
        # validator old                 
        #(metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )
        try:
            netcdf_w = processlib.copyNetCDF(    inputs['netcdf_source'].getValue() ,
                                                 inputs['netcdf_target'].getValue() )

            #content of prov... move...
            for k in netcdf_w.ncattrs():
              v = netcdf_w.getncattr(k)
              if k not in ["bundle","lineage"]:
                content1[str(k).replace(".","_")] = str(v) 

        except Exception, e:
            content1 = {"copy_error": str(e) } 
            pprint (netcdf_w)
            pprint (content1)

            raise e

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_copy"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "KNMI WPS: Copy netcdf" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC netcdf metadata validator. Checks netCDF global ncattributes for relevant metadata fields." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "Copy input: netCDF opendap link." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            # "default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Copy input: netCDF opendap link." ,
                            "type"       : type("String"),
                            #"default"    : "COPY_vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            #"default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "default"    : "COPY4.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "xstuff",
                            "values"     : ""
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function


class KnmiWeightCopyDescriptor( KnmiWebProcessDescriptor ):

    # via terminal
    # http://pc150396.knmi.nl:9080/impactportal/WPS?service=WPS&request=getcapabilities
    # http://pc150396.knmi.nl:9080/impactportal/WPS?service=WPS&request=execute&identifier=knmi_weight&version=1.0.0&storeexecuteresponse=true&netcdf_source=COPY1.nc&weight=1.2&netcdf_target=X1.nc&variable=vDTR&tags=dre


    # override with validation process
    def process_execute_function(self , inputs, callback):

        callback("process_execute_function_weighted_copy")

        pprint(inputs) 

        content1 = {}

        source1 = [inputs['netcdf_source'].getValue()]
        # validator old                 
        #(metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )
        try:
            netcdf_w = processlib.weightNetCDF( inputs['netcdf_source'].getValue()     ,
                                                inputs['weight'].getValue()            ,
                                                inputs['variable'].getValue()          ,
                                                inputs['netcdf_target'].getValue() )

            #prov.output = netcdf_w
            #print netcdf_w

            #content of prov... move...
            for k in netcdf_w.ncattrs():
              v = netcdf_w.getncattr(k)
              if k not in ["bundle","lineage"]:
                content1[str(k).replace(".","_")] = str(v) 

        except Exception, e:
            content1 = {"copy_error": str(e) } 
            pprint (netcdf_w)
            pprint (content1)

            raise e

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_weight"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "KNMI WPS: Copy with weight netcdf" # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC weight." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source" , 
                            "title"      : "Copy input: netCDF opendap surce list." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            # "default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "weight" , 
                            "title"      : "Copy input: netCDF input weight list." ,
                            "type"       : type("String"),
                            "default"    : "100.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "variable" , 
                            "title"      : "Copy input: netCDF input weight list." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Copy input: netCDF opendap link." ,
                            "type"       : type("String"),
                            #"default"    : "COPY_vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            #"default"    : "COPY2.nc",
                            #"default"    : "COPY3.nc",
                            #"default"    : "COPY_A.nc",
                            "default"    : "COPY_B.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Copy input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "xstuff",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self


class KnmiCombineDescriptor( KnmiWebProcessDescriptor ):


    # override with validation process
    def process_execute_function(self , inputs, callback):

        callback("process_execute_function combine")

        pprint(inputs) 

        content1 = {}

        source1 = [inputs['netcdf_source1'].getValue() , inputs['netcdf_source2'].getValue()]
        # validator old                 
        #(metaTestAnswer,content1) = processlib.testMetadata( variables , [inputs['netcdf'].getValue()] )

        op = {  "add"       :np.add,
                "subtract"  :np.subtract,
                "multiply"  :np.multiply,
                "divide"    :np.divide }
        try:
            #operation = np.__getattr__(inputs['operation'].getValue())
            operation = op[inputs['operation'].getValue()]
        except Exception ,e:
            raise "Error exception in operator"

        try:
            netcdf_w = processlib.combineNetCDF( inputs['netcdf_source1'].getValue()      ,
                                                 inputs['variable1'].getValue()           ,
                                                 inputs['netcdf_source2'].getValue()      ,
                                                 inputs['variable2'].getValue()           ,
                                                 inputs['netcdf_target'].getValue()       ,
                                                 operation )

            #prov.output = netcdf_w
            #print netcdf_w

            #content of prov... move...
            for k in netcdf_w.ncattrs():
              v = netcdf_w.getncattr(k)
              if k not in ["bundle","lineage","bundle2","lineage2"]:
                content1[str(k).replace(".","_")] = str(v) 

        except Exception, e:
            content1 = {"copy_error": str(e) } 
            pprint (netcdf_w)
            pprint (content1)

            raise e

        #prov.content.append(content1)
        return content1 , source1, netcdf_w



    def __init__( self ):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_combine"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "KNMI WPS: Combine two inputs into a single netCDF." # = 'SimpleIndices',
        self.structure["abstract"] = "KNMI WPS Process: CLIPC Combine." #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "0.0"
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA D4P"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [
                            { 
                            "identifier" : "netcdf_source1" , 
                            "title"      : "Combine input: netCDF opendap surce list." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            # "default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "variable1" , 
                            "title"      : "Combine input: netCDF input weight list." ,
                            "type"       : type("String"),
                            "default"    : "100.0" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_source2" , 
                            "title"      : "Combine input: netCDF opendap surce list." ,
                            "type"       : type("String"),
                            "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            # "default"    : "COPY1.nc",
                            #"default"    : "COPY2.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "variable2" , 
                            "title"      : "Combine input: netCDF input weight list." ,
                            "type"       : type("String"),
                            "default"    : "vDTR" ,
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "netcdf_target" , 
                            "title"      : "Combine input: netCDF opendap link." ,
                            "type"       : type("String"),
                            #"default"    : "COPY_vDTR_SEP_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc" ,
                            #"default"    : "COPY2.nc",
                            #"default"    : "COPY3.nc",
                            #"default"    : "COPY_A.nc",
                            "default"    : "COPY_B.nc",
                            "values"     : None
                            } ,
                            { 
                            "identifier" : "operation" , 
                            "title"      : "Combine input: Any numpy function." ,
                            "type"       : type("String"),
                            "default"    : "add",
                            "values"     : ["add","subtract","multiply","divide"]
                            } , 
                            { 
                            "identifier" : "tags" , 
                            "title"      : "Combine input: User Defined Tags CLIPC user tags." ,
                            "type"       : type("String"),
                            "default"    : "xstuff",
                            "values"     : None
                            }                   
                          ]


        self.processExecuteCallback = self.process_execute_function

        print self
# class Process(WPSProcess):
#     def __init__(self):
#         # init process
#         WPSProcess.__init__(self,
#                             identifier="clipc_combine_identify", #the same as the file name
#                             title="CLIPC Combine Identify",
#                             version = "1.0",
#                             storeSupported = "true",
#                             statusSupported = "true",
#                           abstract="Lists possible operations for two resources",
#                           grassLocation =False)
        
#         self.inputa = self.addLiteralInput(identifier="inputa",
#                                                 title="Input 1",
#                                                 abstract="application/netcdf",
#                                                 default = "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/tier1_indicators/icclim_cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp45_r1i1p1_SMHI-RCA4_v1-SMHI-DBS43-MESAN-1989-2010/vDTR_OCT_MPI-M-MPI-ESM-LR_rcp45_r1i1p1_SMHI-RCA4_v1-SMHI-DBS43-MESAN-1989-2010_EUR-11_2006-2100.nc",
#                                                 type = type("String"))   
#         self.inputb = self.addLiteralInput(identifier="inputb",
#                                                 title="Input 2",
#                                                 abstract="application/netcdf",
#                                                 default = "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/tier1_indicators/icclim_cerfacs/TNn/MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1/TNn_OCT_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_SMHI-RCA4_v1_EUR-11_2006-2100.nc",
#                                                 type = type("String"))   
       
        
#         self.result = self.addLiteralOutput(identifier = "result",title = "answer");

    
#     def execute(self):
        
#         self.result.setValue("add,substract,divide,multiply");
#         self.status.set("Finished....", 100)      

# generic...
# class KnmiWebProcessDescriptor(object):
# class KnmiCombineIdentify( KnmiWebProcessDescriptor ):


#     # override with validation process
#     def process_execute_function(self , inputs, callback):

#         callback("process_execute_function combine identify")

#         # #pprint(inputs) 
#         answer = "add,substract,divide,multiply"

#         # NOT COMPLETE IN IMPACTPORTAL VERSION....???

#         #content1 = {"content": str(clipc_combine_process.ops) }
#         content1 = {"content": "clipc_combine_process.ops" }
#         source1  = {}
#         for k in inputs.keys():
#             source1.append( { str(k) : inputs[k].getValue() } )

#         self.result.setValue( answer );
#         self.status.set("Finished.", 100)   

#         # nc1 , nc2 , nc_combo = clipc_combine_process.combine_two_indecies_wcs(wcs_url1, wcs_url2, op , norm1 , norm2 , bbox , time1 , time2 , tmpFolderPath+'/wcs_nc1.nc' , tmpFolderPath+'/wcs_nc2.nc', fileOutPath+"/"+outputfile,width=width , height=height, callback=callback ,certfile=certfile)


#         # #The final answer    
#         # url = fileOutURL+"/"+outputfile;
#         # self.result.setValue(url);
#         # self.status.set("Finished....", 100)      
 

#         return content1 , source1, None



#     def __init__( self ):
#         self.structure = {}      
#         self.inputsTuple = []

#         self.structure["identifier"] = "clipc_combine_identify"
#         self.structure["title"]= "CLIPC Combine Identify"
#         self.structure["abstract"] = "KNMI WPS Process: [CLIPC] Lists possible operations for two resources"
#         self.structure["version"] = "1.1"
#         self.structure["storeSupported"] = True
#         self.structure["statusSupported"] = True
#         self.structure["grassLocation"] = False
#         self.structure["metadata"] = "METADATA D4P" 
       
        
#         self.result = self.addLiteralOutput(identifier = "result",title = "answer")

#         # input tuple describes addLiteralInput, values
#         self.inputsTuple = [
#                             { 
#                             "identifier" : "inputa" , 
#                             "title"      : "Input 1" ,
#                             "abstract"   : "application/netcdf",
#                             "type"       : "String",
#                             "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/tier1_indicators/icclim_cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp45_r1i1p1_SMHI-RCA4_v1-SMHI-DBS43-MESAN-1989-2010/vDTR_OCT_MPI-M-MPI-ESM-LR_rcp45_r1i1p1_SMHI-RCA4_v1-SMHI-DBS43-MESAN-1989-2010_EUR-11_2006-2100.nc",
#                             "values"     : None
#                             } ,
#                             { 
#                             "identifier" : "inputb" , 
#                             "title"      : "Input 2" ,
#                             "abstract"   : "application/netcdf",
#                             "type"       : "String",
#                             "default"    : "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/tier1_indicators/icclim_cerfacs/vDTR/MPI-M-MPI-ESM-LR_rcp45_r1i1p1_SMHI-RCA4_v1-SMHI-DBS43-MESAN-1989-2010/vDTR_NOV_MPI-M-MPI-ESM-LR_rcp45_r1i1p1_SMHI-RCA4_v1-SMHI-DBS43-MESAN-1989-2010_EUR-11_2006-2100.nc",
#                             "values"     : None
#                             }                  
#                           ]


#         self.processExecuteCallback = self.process_execute_function