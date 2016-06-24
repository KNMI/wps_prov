# runs wps processes locally
# author: andrej
# project: clipc

import knmi_wps_processes
from knmi_wps_processes import wps_knmi
from knmi_wps_processes import wps_knmi_processes
from pywps.Process import Status

#####knmi_wps_processes
#
# run from run.wps.here.py (this allows the local cgi to be used...)
# author: ANDREJ
# tests provenance with knmi wps.
#

#Override status class and method in order to print to stdout directly.
class MyStatus(Status):
    def set(self,string1,p):
        print string1 

#---
status = MyStatus()

#---
knmi_process = wps_knmi.KnmiWpsProcess(wps_knmi.KnmiWebProcessDescriptor())
knmi_process.status = status
knmi_process.execute()

# #---
knmi_clipc_validation = wps_knmi.KnmiWpsProcess(wps_knmi_processes.KnmiClipcValidationDescriptor())
knmi_clipc_validation.status = status
knmi_clipc_validation.execute()

#--- KnmiCopyDescriptor test
knmi_copy = wps_knmi.KnmiWpsProcess(wps_knmi_processes.KnmiCopyDescriptor())
knmi_copy.status = status
knmi_copy.execute()
