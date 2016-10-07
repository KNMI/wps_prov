import knmi_wps_processes
from knmi_wps_processes import wps_knmi
from knmi_wps_processes import wps_knmi_processes
from pywps.Process import Status, WPSProcess

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



''' https://dev.knmi.nl/projects/clipccombine/wiki '''


'''
class Validate(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiClipcValidationDescriptor() )
'''     
class Copy(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiCopyDescriptor())

class Weight(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiWeightCopyDescriptor())

class Wcs(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiWcsDescriptor())

class Combine(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiCombineDescriptor())

class NormaliseAdvanced(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiNormaliseAdvancedDescriptor())

class NormaliseLinear(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiNormaliseLinearDescriptor())

class AdvancedCombine(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.KnmiAdvancedCombineDescriptor())
'''
class Correlatefield(wps_knmi.KnmiWpsProcess):
    # KnmiWebProcessDescriptor
    def __init__(self):
        wps_knmi.KnmiWpsProcess.__init__(self , wps_knmi_processes.CorrelatefieldDescriptor())
'''