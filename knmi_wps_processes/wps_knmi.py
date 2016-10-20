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

import os
from datetime import datetime
import provenance
import sys, traceback #traceback.print_exc(file=sys.stdout)
import netCDF4
import logging


 
# base descriptor used in wps_knmi_processes
class KnmiWebProcessDescriptor(object):

    ''' generic descriptor for knmi wps processes '''

    # override this function to allow 
    def process_execute_function(self ,inputs, callback, fileOutPath):

        ''' only logs '''
        print "process_execute_function" 
        #logging.error (inputs) 
        logging.error (inputs)

        #print "content: ",prov.content 

        return {'content':'ANDREJ'} , inputs , None

    def __init__(self):
        self.structure = {}      
        self.inputsTuple = []

        self.structure["identifier"] = "knmi_wps"   # = 'wps_simple_indice', # only mandatary attribute = same file name
        self.structure["title"]= "KNMI WPS Process" # = 'SimpleIndices',
        self.structure["abstract"] = "General KNMI WPS consisting of a tupple of inputs (see generic descriptor and process example)" #'Computes single input indices of temperature TG, TX, TN, TXx, TXn, TNx, TNn, SU, TR, CSU, GD4, FD, CFD, ID, HD17; of rainfal: CDD, CWD, RR, RR1, SDII, R10mm, R20mm, RX1day, RX5day; and of snowfall: SD, SD1, SD5, SD50.'
        self.structure["version"] = "1.0.0",
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
                            "values"     : ["TG","TX","TN","TXx","TXn","WOOOOOOOOOOOOOOOW"]
                            }               
                          ]


        self.processExecuteCallback = self.process_execute_function



# generic KNMI process
class KnmiWpsProcess(WPSProcess):

    ''' Generic KNMI WPS Process with provenance enabled: constructed using KnmiWebProcessDescriptor '''
    #def __init__(self):
    #    KnmiWpsProcess.__init__(self, KnmiWebProcessDescriptor() )

    # descirbes WPS
    def __init__(self,descriptor):

        ''' initialise with KnmiWebProcessDescriptor '''
        self.fileOutPath1 = None
        self.fileOutURL = ""
        self.bundle = None

        self.output = None

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

        descriptor.process = self 
        
        
        for inputDict in descriptor.inputsTuple:

            if inputDict.has_key("abstract"): 
                self.inputs[inputDict["identifier"]] = self.addLiteralInput(  identifier = inputDict["identifier"] ,
                                                                              title      = inputDict["title"],
                                                                              type       = inputDict["type"],
                                                                              default    = inputDict["default"], 
                                                                              abstract   = inputDict["abstract"]
                                                                              ) 
            else:
                self.inputs[inputDict["identifier"]] = self.addLiteralInput(  identifier = inputDict["identifier"] ,
                                                                              title      = inputDict["title"],
                                                                              type       = inputDict["type"],
                                                                              default    = inputDict["default"] 
                                                                      ) 
            #######
            #abstract="application/netcdf"
            try:              
                if inputDict["maxOccurs"] is not None:
                    self.inputs[inputDict["identifier"]].maxOccurs = inputDict["maxOccurs"]
            except Exception, e:
                #print "no maxOccurs"
                pass
                
            try:              
                if inputDict["values"] is not None:
                    self.inputs[inputDict["identifier"]].values = inputDict["values"]
            except Exception, e:
                #print "no values"
                pass


        self.processExecuteCallback = descriptor.processExecuteCallback

        self.opendapURL = self.addLiteralOutput(identifier = "opendapURL",title = "opendapURL"); 

    # logging using pywps status module   
    def callback(self,message,percentage):
        ''' status update '''
        self.status.set("%s" % str(message),str(percentage));


       
    # key:    
    # runs WPS
    def execute(self):

        ''' runs wps with provenance '''
        # Very important: This allows the NetCDF library to find the users credentials (X509 cert)
        homedir = os.environ['HOME']
        os.chdir(homedir)


        self.callback( "EXECUTE "+homedir,0)

        ''' file path based on oauth and cert user.'''
        if self.fileOutPath1 is None:
            """ pathToAppendToOutputDirectory """
            # pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
            pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M")

            #self.callback("POF_OUTPUT_URL: "+os.environ['POF_OUTPUT_URL'],1)
            # """ URL output path """
            self.fileOutURL  = os.environ['POF_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
            


            """ Internal output path"""
            self.fileOutPath1 = os.environ['POF_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

            """ Create output directory """
            if not os.path.exists(self.fileOutPath1):
                os.makedirs(self.fileOutPath1)
        # else nothing        

        #self.callback(fileOutURL,10)
        self.callback(self.fileOutPath1,1)    
        # this can be extended for better debug...
        def callback(b,info=""):
            self.callback("Processing wps_knmi "+str(info),b)

        # PE used is dispel4py should be here
        # currently    
        self.callback("KnmiWpsProcess", 2)

        ''' create metadata object, initiate bundle if not existing '''
        # bundle created here
        username = homedir.split("/")[-2]

        # use prov call back later... each start creates lineage info
        prov = provenance.MetadataD4P(  name=self.identifier , 
                                        description="Povenance using D4P for "+self.abstract ,
                                        username=username, #"andrej", 
                                        inputs=self.inputs ,
                                        bundle0=self.bundle 
                                        ) #does wps provide a user id...
        
        self.bundle = prov.bundle
        # MOVED IN PROCESS CAUSES DEPENDACNY... for demo...
        #prov.start( self.inputs ) # use prov call back later... each start creates lineage info
        self.callback("Start wps.", 3)

        ''' run: process_execute_function, defined in descriptor '''
        #try:
        self.callback("Start wps.", 4)
        
        for k in self.inputs.keys():
            self.callback(str(k)+": "+str( self.inputs[k].getValue()), 4)
        
        
        logging.debug("Something has been debugged")

        
        self.callback(str(self.fileOutPath1), 4)
        
        ''' PROCESS OUTPUTs '''
        content, source , fileO = self.processExecuteCallback( self.inputs , callback , self.fileOutPath1 )

        self.netcdf_w = fileO

        size = 0
        
        if fileO is not None:
            self.callback("Finished wps."+str(fileO), 70)
            # try:
            #     size = os.stat(fileO.filepath()).st_size
            # except Exception, e:
            #     content(71,info=str(e))
            

    #except Exception, e:
            #prov.errors(str(e))

            #traceback.print_exc(file=sys.stderr)
            #raise e

        callback(80)


        prov.content = { "prov:type" : "data_element" }
        prov.content.update( content )
        #prov.content = content

        callback(90,info='finish provenance')

        prov.output = fileO  
        
        ''' provenance related can be moved'''
        try:
            outputurl = str(self.fileOutURL)+self.inputs['netcdf_target'].getValue()
        except Exception, e:
            outputurl = str(self.fileOutURL)+"/wpsoutputs"
     

        ''' adds knmi_prov '''
        prov.finish( source , outputurl ,size)  

        ''' finalise prov and write to netcdf '''
        prov.closeProv()

        #logging.debug("self.fileOutURL == "+str(self.fileOutURL))
        self.opendapURL.setValue(outputurl)

        ''' output to local json '''
        prov.writeMetadata('bundle.json')
        self.callback("metadata inserted.", 100)


        #
        # issues with prov library here, need to ironout...
        #xml = provexport.toW3Cprov( [prov.lineage] , [prov.bundle])
        #provenance.writeXML('w3c-prov.xml' , xml )




