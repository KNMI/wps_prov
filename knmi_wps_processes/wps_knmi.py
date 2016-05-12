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



# generic KNMI process
class KnmiWpsProcess(WPSProcess):

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
            self.inputs[inputDict["identifier"]] = self.addLiteralInput(  identifier = inputDict["identifier"] ,
                                                                          title      = inputDict["title"],
                                                                          type       = inputDict["type"],
                                                                          default    = inputDict["default"] 
                                                                          ) 
            self.inputs[inputDict["identifier"]].values = inputDict["values"]

        self.processCallback = descriptor.processCallback

       

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




