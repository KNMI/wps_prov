import wps_knmi
import wps_knmi_processes
from pywps.Process import Status

#
# run from run.wps.here.py (this allows the local cgi to be used...)
# author: ANDREJ
# tests provenance with knmi wps.
#

#Override status class and method in order to print to stdout directly.
class MyStatus(Status):
    def set(self,string1,p):
        print string1 

status = MyStatus()


knmi_process = wps_knmi.KnmiWpsProcess(wps_knmi.KnmiWebProcessDescriptor())
knmi_process.status = status
knmi_process.execute()


knmi_clipc_validation = wps_knmi.KnmiWpsProcess(wps_knmi_processes.KnmiClipcValidationWPS())
knmi_clipc_validation.status = status
knmi_clipc_validation.execute()
