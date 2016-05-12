# CLIPC Provenance
#
#
#   author: Andrej
import os
import json
from pprint import pprint
import datetime
import uuid
import socket
import httplib
import urllib
from urlparse import urlparse
# 

# read json examples from alessandro.

#
def readJSON(file_directory):
    json_data=open(file_directory).read()
    data = json.loads(json_data)
    
    return data

def writeJSON( file_directory, dictionary ):
    with open( file_directory ,'a') as outf:
        json_str = json.dump(dictionary,outf)
        outf.write("\n")

def writeXML( file_directory, xml ):
    with open( file_directory ,'a') as outf:
        outf.write(xml)
        outf.write("\n")

#
def printJSON(file_directory):
    data = readJSON(file_directory)

    pprint (data)

    return data

# 
def printAll():
    prov_json = {}
    for root, dirs, files in os.walk("prov-files"):
        for f in files:
            #prov_list.append(root+"/"+f)
            print ""
            file_directory = root+"/"+f
            print file_directory

            data = readJSON(file_directory)
                # json_data=open(file_directory).read()
                # data = json.loads(json_data)
            pprint(data)    
                # pprint(data)
            #print files , "  " ,len(data.keys()) 

            for k in data.keys():
                if k in prov_json.keys():
                    prov_json[k] += 1
                else:
                    prov_json[k]=1

    pprint(prov_json)

#printAll()

def testJSONs():
    ###
    print "\nLINEAGE"
    lineage = printJSON("prov-files/PE_square_write_orfeus-as-85724-d27f8c4a-0222-11e6-8f71-f45c89acf865")

    ###
    print "\nBUNDLE"
    bundle  = printJSON("prov-files/RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865")

    writeJSON("bundle.json",bundle)


# POST metadata to REPOS_URL
# author: Alessandro Spin

# CLIPC D4P dev repository
REPOS_URL='http://verce-portal-dev.scai.fraunhofer.de/j2ep-1.0/prov/workflow/insert'

def writePOST(provenance_json):
    print ""
    print "writePOST()"
    provurl = urlparse(REPOS_URL)
    connection = httplib.HTTPConnection(provurl.netloc)
    prov = provenance_json
    out = None
    if isinstance(prov, list) and "data" in prov[0]:
        prov = prov[0]["data"]

    #if self.convertToW3C:
    #    out = provexport.toW3Cprov(prov)
    #else:
    out = prov

    pprint(provenance_json)
   
    params = urllib.urlencode({'prov': json.dumps(out)})
    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Accept": "application/json"}
    connection.request(
        "POST",
        provurl.path,
        params,
        headers)

    response = connection.getresponse()
    print("Response From Provenance Serivce: ", response.status,
          response.reason, response, response.read())
    connection.close()
    return None


# Metadata bridge to D4P
# Currently only used to log locally
# 
class MetadataD4P(object):

    # {u'_id': u'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865',
    #  u'description': u'',
    #  u'input': [],
    #  u'mapping': u'-f',
    #  u'runId': u'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865',
    #  u'startTime': u'2016-04-14 09:25:24.422633',
    #  u'system_id': None,
    #  u'type': u'workflow_run',
    #  u'username': u'aspinuso',
    #  u'workflowId': u'xx',
    #  u'workflowName': u'test_rdwd'}

    def __init__(self, name , description, username ):

        #create bundle with new MetadataDP, replace with real D4P soon...
        self.bundle ={}
        self.lineage={}
        self.content={}
        self.process_id = 1
        
        self.bundle['name'] = name 
        
        id_new = "RDWD_"+name +"-"+ str(uuid.uuid4())
        self.bundle['_id'] = id_new #'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865',

        self.bundle['description'] = description #'',
        self.bundle['input'] = [] # may need to be tupple [] parameters of wps service...
        self.bundle['mapping'] = 'simple'
        self.bundle['runId'] = id_new #'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865',
        self.bundle['startTime'] = str(datetime.datetime.utcnow()) #'2016-04-14 09:25:24.422633',
        #self.bundle['system_id'] = None,
        self.bundle['type'] = 'workflow_run'
        self.bundle['username'] = username #'aspinuso',
        #self.bundle['workflowId'] = "xyz" #'xx',
        self.bundle['workflowName'] = 'test_knmi_wps'

    # def __init__(self,id_existing):
    #   self.bundle['_id'] = id_existing #'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865',
    #   self.bundle['description'] = "" #'',
    #   self.bundle['input'] = "" #[],
    #   self.bundle['mapping'] = "" #'-f',
    #   self.bundle['runId'] = "" #'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865',
    #   self.bundle['startTime'] = "" #'2016-04-14 09:25:24.422633',
    #   self.bundle['system_id'] = "" #None,
    #   self.bundle['type'] = "" #'workflow_run',
    #   self.bundle['username'] = "" #'aspinuso',
    #   self.bundle['workflowId'] = "" #'xx',
    #   self.bundle['workflowName'] = "" #'test_rdwd'}

    def createLineage(self , runId , name , inputs):
        # {u'_id': u'PE_square_write_orfeus-as-85724-d27f8c4a-0222-11e6-8f71-f45c89acf865',
        #  u'actedOnBehalfOf': u'PE_square_8',
        #  u'annotations': {},
        # -----------------think here ------------
        #  u'derivationIds': [{u'DerivedFromDatasetID': u'orfeus-as-85724-d27f71e3-0222-11e6-b374-f45c89acf865',
        #                      u'TriggeredByProcessIterationID': u'PE_source-orfeus-as-85724-d27f63b5-0222-11e6-976e-f45c89acf865',
        #                      u'port': u'input'}],
        #-------------------------------------------
        #  u'endTime': u'2016-04-14 09:25:26.509667',
        #  u'errors': u'',
        #  u'instanceId': u'PE_square-Instance--orfeus-as-85724-d27f5d8a-0222-11e6-915e-f45c89acf865',
        #  u'iterationId': u'PE_square-orfeus-as-85724-d27f8940-0222-11e6-b693-f45c89acf865',
        #  u'iterationIndex': 3,
        #  u'mapping': u'-f',
        #  u'name': u'PE_square',
        #  u'parameters': [{u'key': u'prov_cluster', u'val': u'cluster'}],
        #  u'pid': u'85724',
        #  u'prov_cluster': u'cluster',
        #  u'runId': u'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92d-f45c89acf865',
        #  u'startTime': u'2016-04-14 09:25:26.509590',
        #  u'stateful': False,
        #  u'streams': [{u'annotations': [],
        #                u'content': [{u'value_s': u'25'}],
        #                u'format': u'Random float',
        #                u'id': u'orfeus-as-85724-d27f8ab3-0222-11e6-9a10-f45c89acf865',
        #                u'location': u'',
        #                u'port': u'output',
        #                u'size': 24}],
        #  u'type': u'lineage',
        #  u'username': u'aspinuso',
        #  u'worker': u'orfeus-as'}

        _id = name+"-write-"+str(uuid.uuid4()) 
        hostname = socket.gethostname()
        self.process_id = str(os.getpid())


        self.lineage['_id'] = _id #'PE_square_write_orfeus-as-85724-d27f8c4a-0222-11e6-8f71-f45c89acf865'
        self.lineage['actedOnBehalfOf'] = name #'PE_square_8'
        self.lineage['annotations'] = {}
        self.lineage['derivationIds'] = [ { "DerivedFromDatasetID" : str(inputs) , "TriggeredByProcessIterationID": _id } ]
        self.lineage['endTime'] = '' # '2016-04-14 09:25:26.509667'
        self.lineage['errors'] = '' # exception can go here...
        self.lineage['instanceId'] = name+"-instance-"+str(hostname)+"-"+str(os.getpid())+"-"+str(uuid.uuid1()) #'PE_square-Instance--orfeus-as-85724-d27f5d8a-0222-11e6-915e-f45c89acf865'
        self.lineage['iterationId'] = _id #'PE_square-orfeus-as-85724-d27f8940-0222-11e6-b693-f45c89acf865'
        self.lineage['iterationIndex'] = 1
        self.lineage['mapping'] = 'simple'
        self.lineage['name'] = name       # 'PE_square'
        self.lineage['parameters'] = []   # { lineage['key'] = 'prov_cluster', lineage['val'] = 'cluster'} ] # inputs should be here...
        self.lineage['pid'] = self.process_id  #'85724'
        self.lineage['prov_cluster'] = 'vps'
        self.lineage['runId'] = runId #'RDWD_orfeus-as-85724-d140e70c-0222-11e6-92ad-f45c89acf865' #bundle
        self.lineage['startTime'] = str(datetime.datetime.utcnow()) #'2016-04-14 09:25:26.509590'
        self.lineage['stateful'] = False
        self.lineage['streams'] = [] 
        # {
        #                               'annotations':[],
        #                               'content':[self.content],
        #                               'format':'',
        #                               'id': process_id+str(uuid.uuid4()),
        #                               'location':'',
        #                               'port':'output',
        #                               'size':''
        # }]
        self.lineage['type'] = 'lineage'
        self.lineage['username'] = 'clipc_knmi'
        self.lineage['worker'] = hostname



    def start(self,inputs, inputs_dict):
        runId = self.bundle['_id']
        name  = self.bundle['name']

        self.bundle['input'] = inputs

        self.createLineage(runId , name , inputs_dict)

    def finish(self,data,netcdf_content_dict):
        list(self.bundle['input']).append(data)

        self.content = (netcdf_content_dict)

        self.lineage['streams'] = [ 
                                    {
                                        'annotations':[],
                                        'content':[self.content],
                                        'format':'json',
                                        'id': self.process_id+str(uuid.uuid4()), # uuid of netcdf better then generated...
                                        'location':'knmi',
                                        'port':'output',
                                        'size':'666'
                                    }
                                  ]
        
        self.lineage['endTime'] = str(datetime.datetime.utcnow())

    def errors(self, error):
        self.lineage['errors'] = error

    def writeMetadata(self,file_directory):

        #pprint(self.bundle)
        #pprint(self.lineage)

        writeJSON(file_directory,self.bundle)
        writeJSON(file_directory,self.lineage)

        writePOST(self.bundle)
        writePOST(self.lineage)
        