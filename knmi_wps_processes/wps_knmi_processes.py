from wps_knmi import KnmiWebProcessDescriptor
from pywps.Process import Status
from pprint import pprint
import netCDF4
#from clipc_combine_process import clipc_wp8_norm
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

    def checkVariable(self, nc_fid, variable, answer, count1):
          try:
              nc_fid.getncattr(variable)
          except Exception, e:
              count1 += 1
              answer.append(variable)

    # import lib
    # move to lib
    def testMetadata(self, ncattributes, files):
        answer = list([])
          # metadata variables... (ncattributes)

        #print "files: ", len(files)
        content = dict()
        count = 0
        for f in files:
            print f

            nc_fid = netCDF4.Dataset( f,'r')

            for v in ncattributes:
                self.checkVariable(nc_fid,v,answer,count)

        # print "variables failed = " , count
        
        for a in nc_fid.ncattrs():
            content[a] = nc_fid.getncattr(a) 

        return (answer,content)


    # override with validation process
    def process_function(self , inputs, callback, content=None):

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

        # print variables
        # print inputs['netcdf'].getValue()

        (metaTestAnswer,content) = self.testMetadata( variables , [inputs['netcdf'].getValue()] )

        pprint( metaTestAnswer )

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


        self.processCallback = self.process_function


# generic...
# class KnmiWebProcessDescriptor(object):


