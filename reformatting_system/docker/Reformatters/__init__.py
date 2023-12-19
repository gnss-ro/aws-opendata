"""
The exports of this package are 

reformatters
==============================
A dictionary of dictionaries such that reformatters[processing_center][file_type] is a 
pointer to a function the reformats a valid file type file_type from a valid 
processing center processing_center) into an AWS-native format. For example, 
reformatters['ucar']['level1b'](...) will reformat a UCAR conPhs file into the 
AWS level1b defined format.

varnames
==============================
A dictionary with processing centers for keywords and values that are the 
functions that parse input file names to obtain metadata on an occultation 
data file. 

valid_processing_centers
==============================
A list of valid processing centers. 

Note: It is possible to obtain a list of valid file types by asking for the 
keys of reformatters[processing_center]. 

"""

#  Imports. 

import os
import re
import pkgutil
from inspect import getmembers, isfunction

#  Exception handling. 

class Error( Exception ):
    pass

class reformattersError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment

#  Logger. 

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Initialize: Import all reformatters. 
################################################################################

reformatters = {}
varnames = {}
pp = os.getenv( "PYTHONPATH" )

for importer, modname, ispkg in pkgutil.walk_packages( [ "Reformatters", os.path.join( pp, "Reformatters" ) ] ): 
    m = importer.find_module(modname).load_module(modname)
    LOGGER.debug( f"Reformatters: modname={modname}" )

    #  Import varnames parser. 

    varnames.update( { modname: m.varnames } )

    #  Import reformatting functions. 

    functions = getmembers( m, isfunction )
    rec = {}
    for f in functions: 
        s = re.search( "^(level[0-3][a-z])2aws$", f[0] )
        if s: rec.update( { s.group(1): f[1] } )
    rec.update( { 'archiveBucket': m.archiveBucket, 'liveupdateBucket': m.liveupdateBucket } )
    reformatters.update( { modname: rec } )

################################################################################
#  Create list of valid processing centers and a dictionary of valid file types 
#  by processing center. 
################################################################################

valid_processing_centers = sorted( list( reformatters.keys() ) )

