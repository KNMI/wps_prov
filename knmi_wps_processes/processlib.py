# processes library
# extend to other...
import netCDF4
import json
from pprint import pprint
import numpy as np
from clipc_combine_process import clipc_wp8_norm as cn
import logging


# logger = logging.getLogger('server_logger')
# fh = logging.FileHandler('/nobackup/users/mihajlov/impactp/tmp/server.log')
# logger.setLevel(logging.INFO);
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)


# def logger_info(str1):
#   with open('/nobackup/users/mihajlov/impactp/tmp/server.log','a') as f:
#     f.write(str(str1)+"\n")
#   f.close()

# logger_info("doit!")


def checkVariable(nc_fid, variable, answer, count1):
      try:
          nc_fid.getncattr(variable)
      except Exception, e:
          count1 += 1
          answer.append(variable)

# import lib
# move to lib
def testMetadata(ncattributes, files):
    answer = list([])
      # metadata variables... (ncattributes)

    #print "files: ", len(files)
    content = dict()
    count = 0
    for f in files:
        print f

        nc_fid = netCDF4.Dataset( f,'r')

        for v in ncattributes:
            checkVariable(nc_fid,v,answer,count)

    # print "variables failed = " , count
        nc_fid.close()

    for a in nc_fid.ncattrs():
        # content[a] = str(nc_fid.getncattr(a))
         content[str(a).replace(".","_")] = str(nc_fid.getncattr(a))

    return answer,content


def createKnmiProvVar(w_nc_fid):
  try:
    v = w_nc_fid.variables['knmi_provenance']
    #print v
  except Exception, e:
    try:
      v = w_nc_fid.createVariable('knmi_provenance', np.str )
      v.setncattr('bundle' ,'')
      v.setncattr('lineage','')
      v.setncattr('prov-dm','')
    except Exception, e:
      v = w_nc_fid.createVariable('knmi_provenance', 'S1' )
      v.setncattr('bundle' ,'')
      v.setncattr('lineage','')
      v.setncattr('prov-dm','')


def appendHistory(global_vars, histStr):
      try:
        global_vars['history'] = global_vars['history']+"\n"+histStr
      except Exception, e:
        global_vars['history'] = histStr

def copyNetCDF( source_name , target_name):
  try:
    nc_fid = netCDF4.Dataset( source_name , 'r')  

    try:
      w_nc_fid = netCDF4.Dataset(target_name, 'w', format='NETCDF4')

      content = dict()

      for var_name, dimension in nc_fid.dimensions.iteritems():
          w_nc_fid.createDimension(var_name, len(dimension) if not dimension.isunlimited() else None)

      for var_name, ncvar in nc_fid.variables.iteritems():

          outVar = w_nc_fid.createVariable(var_name, ncvar.datatype, ncvar.dimensions )
        
          ad = dict((k , ncvar.getncattr(k) ) for k in ncvar.ncattrs() )

          outVar.setncatts(  ad  )

          outVar[:] = ncvar[:]

      #if w_nc_fid.variables['knmi_provenance'] is None:
      #    w_nc_fid.createVariable('knmi_provenance', 'S' , () )
      #if 'knmi_provenance' not in w_nc_fid.variables.iteritems():
      createKnmiProvVar(w_nc_fid)

      global_vars = dict((k , nc_fid.getncattr(k) ) for k in nc_fid.ncattrs() )
      
      #pprint(global_vars)

      # bundStr = json.dumps(prov.bundle)

      # #print bundStr
      # #print type(global_vars)

      # try:
      #   global_vars['bundle'] = global_vars['bundle']+"\n"+ bundStr
      # except Exception, e:
      #   print e
      #   global_vars['bundle'] = bundStr

      # histStr = "copyNetCDF knmi: "+target_name+" to "+source_name
      # try:
      #   global_vars['history'] = global_vars['history']+"\n"+histStr
      # except Exception, e:
      #   global_vars['history'] = histStr
      appendHistory(global_vars,"knmi copy from "+source_name)
      
      #pprint(global_vars)

      for k in sorted(global_vars.keys()):
          v = global_vars[k] 
          w_nc_fid.setncattr(  k , v )

    except Exception, e:
      print "exception writing: ",target_name
      raise e

    nc_fid.close()  

  except Exception, e:
    print "exception reading: ",source_name
    raise e

  

  return w_nc_fid




def weightNetCDF( source_name , weight , layer , target_name):

  ''' weighted netcdf function, allows normalistaion and weight '''
  try:
      nc_fid = netCDF4.Dataset( source_name , 'r') 
  except Exception, e:
    raise e

  try:
    w_nc_fid = netCDF4.Dataset(target_name, 'w', format='NETCDF4')

    content = dict()

    for var_name, dimension in nc_fid.dimensions.iteritems():
        w_nc_fid.createDimension(var_name, len(dimension) if not dimension.isunlimited() else None)


    for var_name, ncvar in nc_fid.variables.iteritems():
        outVar = w_nc_fid.createVariable(var_name, ncvar.datatype, ncvar.dimensions)
      
        ad = dict((k , ncvar.getncattr(k) ) for k in ncvar.ncattrs() )

        outVar.setncatts(  ad  )
        
        # astype('f4')
        if var_name == layer:   
          try:
            if weight in cn.nrm.keys():
              outVar[:] = cn.norm(ncvar[:],weight)
            else:  
              outVar[:] = float(weight) * ncvar[:]
          except Exception, e:
            raise e
        elif var_name != 'knmi_provenance': 
          try:
            outVar[:] = ncvar[:]
          except Exception as e:
            """ Fill value for string is not oK """
            outVar = ncvar
            pass
        
    createKnmiProvVar(w_nc_fid)

    global_vars = dict((k , nc_fid.getncattr(k) ) for k in nc_fid.ncattrs() )
    
    #pprint(global_vars)

    # bundStr = json.dumps(prov.bundle)
    
    # #print bundStr
    # #print type(global_vars)

    # try:
    #   global_vars['bundle'] = global_vars['bundle']+"\n"+ bundStr
        # except Exception, e:
    #   print e
    #   global_vars['bundle'] = bundStr

    # histStr = "weightNetCDF knmi: "+target_name+"weight of "+weight+" to "+source_name
    # try:
    #   global_vars['history'] = global_vars['history']+"\n"+histStr
    # except Exception, e:
    #   global_vars['history'] = histStr
    appendHistory(global_vars,"knmi scale weight:"+weight+" data:"+source_name)  
     
    for k in sorted(global_vars.keys()):
        v = global_vars[k] 
        if "DODS" not in k:
          w_nc_fid.setncattr(  k , v )

    nc_fid.close()

  except Exception, e:
    raise e

  return w_nc_fid


def combineNetCDF( source_name1 , layer1 , source_name2 , layer2 , target_name, operation):

  #print "weightNetCDF"
  # print source_name
  # print weight
  # print target_name
  # print "START"

  try:
      nc_fid1 = netCDF4.Dataset( source_name1 , 'r') 
      nc_fid2 = netCDF4.Dataset( source_name2 , 'r') 

      try:
        w_nc_fid = netCDF4.Dataset(target_name, 'w', format='NETCDF4')
 
        content = dict()

        for var_name, dimension in nc_fid1.dimensions.iteritems():
           
            w_nc_fid.createDimension(var_name, len(dimension) if not dimension.isunlimited() else None)

        for var_name, ncvar in nc_fid1.variables.iteritems():
          
            outVar = w_nc_fid.createVariable(var_name, ncvar.datatype, ncvar.dimensions)
          
            ad = dict((k , ncvar.getncattr(k) ) for k in ncvar.ncattrs() )

            outVar.setncatts(  ad  )

            # astype('f4')
            if var_name == layer1:
                outVar[:] = operation(ncvar[:] , nc_fid2.variables[layer2][:])  
            elif var_name != 'knmi_provenance' :
                try:
                    outVar[:] = ncvar[:]
                except Exception as e:
                    outVar = ncvar
                pass

        createKnmiProvVar(w_nc_fid)

        global_vars  = dict((k , nc_fid1.getncattr(k) ) for k in nc_fid1.ncattrs() )
        global_vars2 = dict((k , nc_fid2.getncattr(k) ) for k in nc_fid2.ncattrs() )

        # histStr = "COMBINE NetCDF knmi: "+target_name+"weight of "+source_name1+" to "+source_name2
        # try:
        #   global_vars['history'] = global_vars['history']+"\n"+histStr
        # except Exception, e:
        #   global_vars['history'] = histStr
        appendHistory(global_vars,"knmi combine source1:"+source_name1+" and source2:"+source_name2)  
  
          

        try:
          for k in sorted(global_vars.keys()):
              v = global_vars[k] 
              if k in ['lineage','bundle']:
                w_nc_fid.variables['knmi_provenance'].setncattr(  k , v )
              else:
                w_nc_fid.setncattr(  k , v )

          for k in sorted(global_vars2.keys()):
              v = global_vars2[k] 
              if k in ['lineage','bundle']:
                w_nc_fid.variables['knmi_provenance'].setncattr(  k+"2" , v )
              else:
                #w_nc_fid.setncattr(  k+"2" , v )
 
                if( k not in global_vars.keys() ):
                  w_nc_fid.setncattr(  k+"2" , v )
                elif( global_vars[k] != v ):
                  w_nc_fid.setncattr(  k+"2" , v ) 


        except Exception, e:
          raise e
      except Exception, e:
        raise e

      nc_fid1.close()
      nc_fid2.close()

  except Exception, e:
    raise e

  return w_nc_fid

from scipy import stats

def normaliseAdvancedNetCDF( source_name , min0 , max0 , centre0 , layer , target_name):

  ''' advanced normalistaion, tudo request. '''
  try:
      nc_fid = netCDF4.Dataset( source_name , 'r') 
  except Exception, e:
      raise e
 
  try:
    w_nc_fid = netCDF4.Dataset(target_name, 'w', format='NETCDF4')
    
    content = dict()

    for var_name, dimension in nc_fid.dimensions.iteritems():
        w_nc_fid.createDimension(var_name, len(dimension) if not dimension.isunlimited() else None)
 
    for var_name, ncvar in nc_fid.variables.iteritems():
        outVar = w_nc_fid.createVariable(var_name, ncvar.datatype, ncvar.dimensions)
      
        ad = dict((k , ncvar.getncattr(k) ) for k in ncvar.ncattrs() )

        outVar.setncatts(  ad  )
        
        # astype('f4')
        if var_name == layer:   
          try:
            # for all values of ncvar, NaN < min, NaN > max ncvar/float(weight)
            
            #outVar[:] = np.clip(ncvar[:],float(min0),float(max0) )# does not set other value...    
            #outVar[:] = [ np.nan if a < float(min0) else a for a in ncvar[:] ] # for array need matrix option
            
            # floats used
            max1 =float(max0)
            min1 =float(min0)
            cen1 =float(centre0)            
            
            # threshold
            # keep mid values
            outVar[:] = stats.threshold(ncvar[:], threshmin=min1, threshmax=max1, newval=np.nan)

            if max1 == cen1 :
              b_mn =  1.0 / ( cen1 - min1 )  
              a_mn = - min1 * b_mn

              def normalise_tudo(x):
                  return b_mn * x + a_mn 
            elif cen1 == min1:
              b_mx = -1.0 / ( max1 - cen1 )   
              a_mx = - max1 * b_mx

              def normalise_tudo(x):
                  return b_mx * x + a_mx 
            else:    
              b_mn =  1.0 / ( cen1 - min1 )
              b_mx = -1.0 / ( max1 - cen1 )

              a_mn = - min1 * b_mn
              a_mx = - max1 * b_mx

              def normalise_tudo(x):
                  return b_mn * x + a_mn  if x < cen1 else b_mx * x + a_mx
            
            nt = np.vectorize(normalise_tudo)

            outVar[:] = nt(outVar[:])

          except Exception, e:
            raise e   
        elif var_name != 'knmi_provenance': 
            try:
                outVar[:] = ncvar[:]
            except Exception as e:
                outVar = ncvar
            pass
        
    createKnmiProvVar(w_nc_fid)

    global_vars = dict((k , nc_fid.getncattr(k) ) for k in nc_fid.ncattrs() )
    

    #istStr = "knmi normalise by threshold min: "+min0+"max: "+max0+"centre: "+centre0+" to "+source_name
    # try:
    #   global_vars['history'] = global_vars['history']+"\n"+histStr
    # except Exception, e:
    #   global_vars['history'] = histStr
    appendHistory(global_vars,"knmi normalise by threshold min: "+min0+"max: "+max0+"centre: "+centre0+" to "+source_name) 
     
    for k in sorted(global_vars.keys()):
        v = global_vars[k] 
        w_nc_fid.setncattr(  k , v )

    nc_fid.close()

  except Exception, e:
    raise e

  return w_nc_fid


import urllib2
import urllib
import xml.etree.ElementTree as et

def getWCS(   wcs_url1, 
              bbox, 
              time, 
              output_file,
              width=300,
              height=300,
              certfile=None):
 

      # Describe Coverage: used to id layer,
      # data also available in getCapabilities...
      values_describe = [  ('SERVICE' , 'WCS'), ('REQUEST' , 'DescribeCoverage') ]
      data_describe = urllib.urlencode(values_describe)
      request_describe =  wcs_url1 + "&" + str(data_describe)

      #print request_describe

      #print request_describe
      if certfile != None:
        opener = urllib.URLopener(key_file =certfile, cert_file = certfile)
        response = opener.open(request_describe)
      else:
        response = urllib2.urlopen( request_describe )
              
      xmlresponse = response.read()
      tree = et.fromstring(xmlresponse)

      for i in tree.iter():
        if 'name' in str(i):
          title = i.text
          break

      # # desribe coordinate ref system    
      crs = 'EPSG:4326'
      #   #'EPSG:3575'
      # #'EPSG:28992' 
      # #'EPSG:4326'

      # # use standard bbox...
      # # TODO if bbox provided check and use...
      # if bbox is None:
      #   bbox = crsbbox.getBBOX(crs)

      # get coverage based on layer described in Describe coverage.
      values = [    ('SERVICE' , 'WCS'),
                    ('REQUEST' , 'GetCoverage') ,
                    ('COVERAGE', title),
                    ('CRS'     , crs ),  
                    ('FORMAT'  , 'netcdf') ,
                    ('BBOX'    , bbox ),
                    ('WIDTH' , width ),
                    ('HEIGHT', height ) 
                ]

      if time is not None:
        values.append( ('TIME', time))    

      data = urllib.urlencode(values)

      request =  wcs_url1 + "&" + str(data)
      logging.debug("WCS Request: "+request);
      logging.debug("WCS: writing to "+str(output_file));
    
      if certfile != None:
        opener = urllib.URLopener(key_file =certfile, cert_file = certfile)
        response = opener.open(request)
      else:
        response = urllib2.urlopen( request )

      output = output_file
      out = open( output , 'wb')
      out.write( bytes(response.read() ) )
      out.close()

      return output_file
