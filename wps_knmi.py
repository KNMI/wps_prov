import pywps
from pywps.Process import WPSProcess

# project: CLIPC
# author: ANDREJ
# adapting wps from c4i to provenance culture.

# generic KnmiWpsProcess
# only initiate with discriptor

# add calculation packages
#import icclim
#import icclim.util.callback as callback
#import dateutil.parser
#from datetime import datetime

#import os
#from os.path import expanduser
#from mkdir_p import *
#transfer_limit_Mb = 100
import provenance
from pprint import pprint
import netCDF4
import provexport

#KnmiWebProcessDescriptor
 
# base descriptor used in wps_knmi_processes
class KnmiWebProcessDescriptor(object):

    # override this function to allow 
    def process_function(self ,inputs, callback, content=None):
        print "process_function" 
        #pprint (inputs) 
        pprint (inputs)
        print "content: ",content 

    def __init__(self):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_wps"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "KNMI WPS Process" # = 'SimpleIndices',
        self.structure["abstract"] = "General KNMI WPS consisting of a tupple of inputs (see generic descriptor and process example)" #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "0.0",
        self.structure["storeSupported"] = True
        self.structure["statusSupported"] = True
        self.structure["grassLocation"] = False
        self.structure["metadata"] = "METADATA YAAAAAAAAAAAA"

        # input tuple describes addLiteralInput, values
        self.inputsTuple = [ 
                            { 
                            "identifier" : "inputName1" , 
                            "title"      : "KNMI Input Name 1" ,
                            "type"       : "String",
                            "default"    : "TN" ,
                            "values"     : ["TG","TX","TN","TXx","TXn","TNx"]
                            },
                            { 
                            "identifier" : "inputName2" , 
                            "title"      : "KNMI Input Name 2" ,
                            "type"       : "String",
                            "default"    : "SU" ,
                            "values"     : ["TG","TX","TN","TXx","TXn","TNx"]
                            },
                            { 
                            "identifier" : "inputName3" , 
                            "title"      : "KNMI Input Name 3" ,
                            "type"       : "String",
                            "default"    : "SU" ,
                            "values"     : ["TG","TX","TN","TXx","TXn","TNx"]
                            }               
                          ]


        self.processCallback = self.process_function




class KnmiWpsProcess(WPSProcess):
    # def __init__(self):
    #     WPSProcess.__init__(self,
    #                         identifier = 'wps_simple_indice', # only mandatary attribute = same file name
    #                         title = 'SimpleIndices',
    #                         abstract = 'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.',
    #                         version = "1.0",
    #                         storeSupported = True,
    #                         statusSupported = True,
    #                         grassLocation =False)

    def __init__(self,descriptor):
        WPSProcess.__init__(self,
                            identifier      = descriptor.structure["identifier"], 
                            title           = descriptor.structure["title"],
                            abstract        = descriptor.structure["abstract"],
                            version         = descriptor.structure["version"],
                            storeSupported  = descriptor.structure["storeSupported"],
                            statusSupported = descriptor.structure["statusSupported"],
                            grassLocation   = descriptor.structure["grassLocation"]
                            )

        self.descriptor = descriptor

        for inputDict in descriptor.inputsTuple:
            self.inputs[inputDict["identifier"]] = self.addLiteralInput(identifier = inputDict["identifier"] ,
                                                                          title      = inputDict["title"],
                                                                          type       = inputDict["type"],
                                                                          default    = inputDict["default"] 
                                                                          ) 
            self.inputs[inputDict["identifier"]].values = inputDict["values"]

        self.processCallback = descriptor.processCallback

       

        # self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
        #                                        title = 'Indice name',
        #                                        type="String",
        #                                        default = 'SU')        

        # self.indiceNameIn.values = ["TG","TX","TN","TXx","TXn","TNx","TNn","SU","TR","CSU","GD4","FD","CFD","ID","HD17","CDD","CWD","PRCPTOT","RR1","SDII","R10mm","R20mm","RX1day","RX5day","SD","SD1","SD5cm","SD50cm"]


        # self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
        #                                       title = 'Slice mode (temporal grouping to apply for calculations)',
        #                                       type="String",
        #                                       default = 'year')
        # self.sliceModeIn.values = ["year","month","ONDJFM","AMJJAS","DJF","MAM","JJA","SON"]


        # self.thresholdIn = self.addLiteralInput(identifier = 'threshold', 
        #                                        title = 'Threshold(s) for certain indices (SU, CSU and TR). Can be a comma separated list, e.g. 20,21,22',
        #                                        type=type("S"),
        #                                        minOccurs=0,
        #                                        maxOccurs=1024,
        #                                        default = None)

       
        # self.filesIn = self.addLiteralInput(identifier = 'files',
        #                                        title = 'Input netCDF files list',
        #                                        abstract="application/netcdf",
        #                                        type=type("S"),
        #                                        minOccurs=0,
        #                                        maxOccurs=1024,
        #                                        default = 'http://aims3.llnl.gov/thredds/dodsC/cmip5_css02_data/cmip5/output1/CMCC/CMCC-CM/rcp85/day/atmos/day/r1i1p1/tasmax/1/tasmax_day_CMCC-CM_rcp85_r1i1p1_20060101-20061231.nc')
        
                                                
        # self.varNameIn = self.addLiteralInput(identifier = 'varName',
        #                                        title = 'Variable name to process',
        #                                        type="String",
        #                                        default = 'tasmax')
        

        # self.timeRangeIn = self.addLiteralInput(identifier = 'timeRange', 
        #                                        title = 'Time range, e.g. 2010-01-01/2012-12-31',
        #                                        type="String",
        #                                         default = '2006-01-01/2006-12-31')
        
        # self.outputFileNameIn = self.addLiteralInput(identifier = 'outputFileName', 
        #                                        title = 'Name of output netCDF file',
        #                                        type="String",
        #                                        default = 'out_icclim.nc')
        
        
        # self.NLevelIn = self.addLiteralInput(identifier = 'NLevel', 
        #                                        title = 'Number of level (if 4D variable)',
        #                                        type="String",
        #                                        default = None)

        # self.opendapURL = self.addLiteralOutput(identifier = "opendapURL",title = "opendapURL");   
        
    def callback(self,message,percentage):
        self.status.set("%s" % str(message),str(percentage));

    
    def execute(self):
        # Very important: This allows the NetCDF library to find the users credentials (X509 cert)
        # homedir = os.environ['HOME']
        # os.chdir(homedir)
        

        def callback(b):
            self.callback("Processing",b)
        
        # bundle created here
        # use prov call back later... each start creates lineage info
        prov = provenance.MetadataD4P(  name=self.identifier , 
                                        description="Povenance using D4P for "+self.abstract ,
                                        username="andrej" ) #does wps provide a user id...
        prov_input = []
        prov_dict = {}
        for k in self.inputs:
            v = self.inputs[k].getValue()
            prov_input.append( (k+'='+v) )
            prov_dict[k] = v

        prov.start(prov_input , prov_dict ) # use prov call back later... each start creates lineage info
        
        netcdf_content_dict = {}
        try:
            self.processCallback( self.inputs , callback , content=netcdf_content_dict )
        except Exception, e:
            prov.errors(str(e))
            raise e

        #prov.content     
            


        prov.finish( self.descriptor.structure,netcdf_content_dict)  

        prov.writeMetadata('bundle.json')


        #xml = provexport.toW3Cprov( [prov.lineage] , [prov.bundle])
        #provenance.writeXML('w3c-prov.xml' , xml )

        # files = [];
        # files.extend(self.filesIn.getValue())
        # var = self.varNameIn.getValue()
        # indice_name = self.indiceNameIn.getValue()
        # slice_mode = self.sliceModeIn.getValue()
        # time_range = self.timeRangeIn.getValue()
        # out_file_name = self.outputFileNameIn.getValue()
        # level = self.NLevelIn.getValue()
        # thresholdlist = self.thresholdIn.getValue()
        # thresh = None
        
        # if(level == "None"):
        #     level = None
            
          
        # if(time_range == "None"):
        #     time_range = None
        # else:
        #     startdate = dateutil.parser.parse(time_range.split("/")[0])
        #     stopdate  = dateutil.parser.parse(time_range.split("/")[1])
        #     time_range = [startdate,stopdate]
            
          
        # if(thresholdlist != "None"):
        #     if(thresholdlist[0]!="None"):
        #         thresh = []
        #         for threshold in thresholdlist:
        #             thresh.append(float(threshold))
        
      
        # self.status.set("Preparing....", 0)
        
        # pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
        
        # """ URL output path """
        # fileOutURL  = os.environ['POF_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
        
        # """ Internal output path"""
        # fileOutPath = os.environ['POF_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

        # """ Create output directory """
        # mkdir_p(fileOutPath)
        

        # self.status.set("Processing input list: "+str(files),0)
        
        # icclim.indice(indice_name=indice_name,
        #                 in_files=files,
        #                 var_name=var,
        #                 slice_mode=slice_mode,
        #                 time_range=time_range,
        #                 out_file=fileOutPath+out_file_name,
        #                 threshold=thresh,
        #                 N_lev=level,
        #                 transfer_limit_Mbytes=transfer_limit_Mb,
        #                 callback=callback,
        #                 callback_percentage_start_value=0,
        #                 callback_percentage_total=100,
        #                 base_period_time_range=None,
        #                 window_width=5,
        #                 only_leap_years=False,
        #                 ignore_Feb29th=True,
        #                 interpolation='hyndman_fan',
        #                 netcdf_version='NETCDF4_CLASSIC',
        #                 out_unit='days')
        
        # """ Set output """
        # url = fileOutURL+"/"+out_file_name;
        # self.opendapURL.setValue(url);
        # self.status.set("ready",100);
        


