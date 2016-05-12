import exceptions
import traceback
from prov.model import ProvDocument, Namespace, Literal, PROV, Identifier
import datetime
import uuid
import traceback
import os
import socket
import json
import csv
import StringIO
from urlparse import urlparse
from itertools import chain


def makeHashableList(listobj,field):
     listobj=[x[field] for x in listobj]
     return listobj

def clean_empty(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_empty(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_empty(v)) for k, v in d.items()) if v}

  
    

def toW3Cprov(ling,bundl,format='w3c-prov-xml'):
        
        g = ProvDocument()
        vc = Namespace("knmi", "http://knmi.nl")  # namespaces do not need to be explicitly added to a document
        con = Namespace("dfp", "http://dispel4py.org")
        g.add_namespace("dcterms", "http://purl.org/dc/terms/")
        
        'specify bundle'
        bundle=None
        for trace in bundl:
            'specifing user'
            ag=g.agent(vc[trace["username"]],other_attributes={"dcterms:author":trace["username"]})  # first time the ex namespace was used, it is added to the document automatically
            
            if trace['type']=='workflow_run':
                
                trace.update({'runId':trace['_id']})
                bundle=g.bundle(vc[trace["runId"]])
                bundle.actedOnBehalfOf(vc[trace["runId"]], vc[trace["username"]])
                
                dic={}
                i=0
                
                for key in trace:
                    
                
                    if key != "input": 
                        if ':' in key:
                            dic.update({key: trace[key]})
                        else:
                            dic.update({vc[key]: trace[key]})
                    
            
                dic.update({'prov:type': PROV['Bundle']})
                g.entity(vc[trace["runId"]], dic)
                
                dic={}
                i=0
                if type(trace['input'])!=list:
                    trace['input']=[trace['input']]
                for y in trace['input']:
                    for key in y:
                        if ':' in key:
                            dic.update({key: y[key]})
                        else:
                            dic.update({vc[key]: y[key]})
                    dic.update({'prov:type': 'worklfow_input'})
                    bundle.entity(vc[trace["_id"]+"_"+str(i)], dic)
                    bundle.used(vc[trace["_id"]], vc[trace["_id"]+"_"+str(i)], identifier=vc["used_"+trace["_id"]+"_"+str(i)])
                    i=i+1
                    
                    
        'specify lineage'
        for trace in ling:

            #pprint(trace)

            try:
                bundle=g.bundle(vc[trace["runId"]])
                bundle.wasAttributedTo(vc[trace["runId"]], vc["ag_"+trace["username"]],identifier=vc["attr_"+trace["runId"]])
            
            except:
                pass
            'specifing creator of the activity (to be collected from the registy)'
        
            if 'creator' in trace:
                bundle.agent(vc["ag_"+trace["creator"]],other_attributes={"dcterms:creator":trace["creator"]})  # first time the ex namespace was used, it is added to the document automatically
                bundle.wasAssociatedWith('process_'+trace["iterationId"],vc["ag_"+trace["creator"]])
                bundle.wasAttributedTo(vc[trace["runId"]], vc["ag_"+trace["creator"]])
    
            'adding activity information for lineage'
            dic={}
            for key in trace:
                
                if type(trace[key])!=list:
                    if ':' in key:
                        dic.update({key: trace[key]})
                    else:
                        
                        if key=='location':
                            
                            dic.update({"prov:location": trace[key]})    
                        else:
                            dic.update({vc[key]: trace[key]})
            bundle.activity(vc["process_"+trace["iterationId"]], trace["startTime"], trace["endTime"], dic.update({'prov:type': trace["name"]}))
        
            'adding parameters to the document as input entities'
            dic={}
            for x in trace["parameters"]:
                #print x
                if ':' in x["key"]:
                    dic.update({x["key"]: x["val"]})
                else:
                    dic.update({vc[x["key"]]: x["val"]})
                
            dic.update({'prov:type':'parameters'})        
            bundle.entity(vc["parameters_"+trace["instanceId"]], dic)
            bundle.used(vc['process_'+trace["iterationId"]], vc["parameters_"+trace["instanceId"]], identifier=vc["used_"+trace["iterationId"]])

            'adding input dependencies to the document as input entities'
            dic={}
        
            for x in trace["derivationIds"]:
                'state could be added'   
            #dic.update({'prov:type':'parameters'})        
                bundle.used(vc['process_'+trace["iterationId"]], vc[x["DerivedFromDatasetID"]], identifier=vc["used_"+x["DerivedFromDatasetID"]])


            'adding entities to the document as output metadata'
            for x in trace["streams"]:
                i=0
                parent_dic={}
                for key in x:
                        if key=='con:immediateAccess':
                            
                            parent_dic.update({vc['immediateAccess']: x[key]}) 
                            
                    
                        elif key=='location':
                             
                            parent_dic.update({"prov:location": str(x[key])})    
                        else:
                            parent_dic.update({vc[key]: str(x[key])})    
            
            
                c1=bundle.collection(vc[x["id"]],other_attributes=parent_dic)
                bundle.wasGeneratedBy(vc[x["id"]], vc["process_"+trace["iterationId"]], identifier=vc["wgb_"+x["id"]])
            
                for d in trace['derivationIds']:
                      bundle.wasDerivedFrom(vc[x["id"]], vc[d['DerivedFromDatasetID']],identifier=vc["wdf_"+x["id"]])
        
                for y in x["content"]:
                
                    dic={}
                
                    if isinstance(y, dict):
                        val=None
                        for key in y:
                        
                            try: 
                                val =num(y[key])
                                
                            except Exception,e:
                                val =str(y[key])
                            
                            if ':' in key:
                                dic.update({key: val})
                            else:
                                dic.update({vc[key]: val})
                    else:
                        dic={vc['text']:y}
                
                 
                    dic.update({"verce:parent_entity": vc["data_"+x["id"]]})
                    
                    print  x["id"]
                    print  str(i)
                    print  dic

                    e1=bundle.entity(vc["data_"+x["id"]+"_"+str(i)], dic)
                
                    bundle.hadMember(c1, e1)
                    bundle.wasGeneratedBy(vc["data_"+x["id"]+"_"+str(i)], vc["process_"+trace["iterationId"]], identifier=vc["wgb_"+x["id"]+"_"+str(i)])
                
                    for d in trace['derivationIds']:
                        bundle.wasDerivedFrom(vc["data_"+x["id"]+"_"+str(i)], vc[d['DerivedFromDatasetID']],identifier=vc["wdf_"+"data_"+x["id"]+"_"+str(i)])
        
                    i=i+1
            
    
         
        
        if format =='w3c-prov-json':
            return str(g.serialize(format='json'))
        elif format=='png':
            output = StringIO.StringIO()
            g.plot('test.png')
            return output
            
        else:
            return g.serialize(format='xml')
