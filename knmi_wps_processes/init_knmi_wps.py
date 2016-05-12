import wps_knmi
import wps_knmi_processes
from pywps.Process import Status

#
# run from run.wps.here.py (this allows the local cgi to be used...)
# author: ANDREJ
# tests provenance with knmi wps.
#

# target this function with __init__.py from the wps.py process.

#Override status class and method in order to print to stdout directly.
class MyStatus(Status):
    def set(self,string1,p):
        print string1 
status = MyStatus()


# knmi_process = wps_knmi.KnmiWpsProcess()
# knmi_process.status = status
# knmi_process.execute()

class GenericKnmiWpsProcess(KnmiWpsProcess): 
    def __init__(self):
        KnmiWpsProcess.__init__(wps_knmi.KnmiWpsProcess())
    #wps_knmi.KnmiWebProcessDescriptor()


# knmi_clipc_validation = wps_knmi.KnmiWpsProcess(wps_knmi_processes.KnmiClipcValidationDescriptor())
# knmi_clipc_validation.status = status
# knmi_clipc_validation.execute()
class KnmiWpsClipcValidation(KnmiWpsProcess):
    def __init__(self):
        KnmiWpsProcess.__init__(wps_knmi_processes.KnmiClipcValidationDescriptor())

