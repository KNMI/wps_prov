# processes library
# extend to other...
import netCDF4
import json
from pprint import pprint
import numpy as np
import clipc_wp8_norm as cn

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
        #nc_fid.close()

    for a in nc_fid.ncattrs():
        # content[a] = str(nc_fid.getncattr(a))
         content[str(a).replace(".","_")] = str(nc_fid.getncattr(a))

    return answer,content




# def copyNetCDF(name, nc_fid , des ):
#   w_nc_fid = netCDF4.Dataset(name, 'w', format='NETCDF4')

#   #PROVENANCE ADDED!!!!

#   w_nc_fid.description = des

#   for var_name, dimension in nc_fid.dimensions.iteritems():
#     w_nc_fid.createDimension(var_name, len(dimension) if not dimension.isunlimited() else None)

#   for var_name, ncvar in nc_fid.variables.iteritems():

#     outVar = w_nc_fid.createVariable(var_name, ncvar.datatype, ncvar.dimensions )
  
#     ad = dict((k , ncvar.getncattr(k) ) for k in ncvar.ncattrs() )

#     outVar.setncatts(  ad  )

#     outVar[:] = ncvar[:]

#   global_vars = dict((k , nc_fid.getncattr(k) ) for k in nc_fid.ncattrs() )
  
#   for k in sorted(global_vars.keys()):
#     w_nc_fid.setncattr(  k , global_vars[k]  )

#   return w_nc_fid

def createKnmiProvVar(w_nc_fid):
  try:
    v = w_nc_fid.variables['knmi_provenance']
    print v
  except Exception, e:
    v = w_nc_fid.createVariable('knmi_provenance', np.str )
    v.setncattr('bundle' ,'')
    v.setncattr('lineage','')
    v.setncattr('prov-dm','')



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

      histStr = "copyNetCDF knmi: "+target_name+" to "+source_name
      try:
        global_vars['history'] = global_vars['history']+"\n"+histStr
      except Exception, e:
        global_vars['history'] = histStr

      
      #pprint(global_vars)

      for k in sorted(global_vars.keys()):
          v = global_vars[k] 
          w_nc_fid.setncattr(  k , v )

    except Exception, e:
      print "exception writing: ",target_name
      raise e

  except Exception, e:
    print "exception reading: ",source_name
    raise e


  return w_nc_fid

def weightNetCDF( source_name , weight , layer , target_name):

  #print "weightNetCDF"
  # print source_name
  # print weight
  # print target_name
  # print "START"

  try:
      nc_fid = netCDF4.Dataset( source_name , 'r') 

      #print "read worked"

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
              outVar[:] = ncvar[:]
            
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
  
        histStr = "weightNetCDF knmi: "+target_name+"weight of "+weight+" to "+source_name
        try:
          global_vars['history'] = global_vars['history']+"\n"+histStr
        except Exception, e:
          global_vars['history'] = histStr

         
        for k in sorted(global_vars.keys()):
            v = global_vars[k] 
            w_nc_fid.setncattr(  k , v )

      except Exception, e:
        print "exception writing: ",target_name
        raise e

 
  except Exception, e:
    print "exception reading: ",source_name
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
              outVar[:] = ncvar[:]

        createKnmiProvVar(w_nc_fid)

        global_vars  = dict((k , nc_fid1.getncattr(k) ) for k in nc_fid1.ncattrs() )
        global_vars2 = dict((k , nc_fid2.getncattr(k) ) for k in nc_fid2.ncattrs() )

        histStr = "COMBINE NetCDF knmi: "+target_name+"weight of "+source_name1+" to "+source_name2
        try:
          global_vars['history'] = global_vars['history']+"\n"+histStr
        except Exception, e:
          global_vars['history'] = histStr


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
              w_nc_fid.setncattr(  k+"2" , v )

      except Exception, e:
        print "Exception: ",target_name
        raise e

  except Exception, e:
    print "Exception: ",source_name1
    print "Exception: ",source_name2
    raise e


  return w_nc_fid