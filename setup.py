#!/usr/bin/env python

from distutils.core import setup

setup(name='ProvenanceNetcdf',
      version='1.0',
      description='Provenance tools used by PyWPS with NetCDFs',
      author='Andrej Mihajlovski, Alessandro Spinuso',
      author_email='clipc@knmi.nl',
      packages=['knmi_wps_processes'], #dispel4py is also localy used...
      )
