"""
The exports of this package are

versions
==============================
A list of dictionaries wherein each element dictionary has a pointer to the
AWS version (key "AWSversion") and a pointer to the FileFormatters module
(key "FileFormatters").

get_version
==============================
Return the element version of Versions.versions corresponding to a string
AWS version identifier.
"""

#  Imports.

import re
import os
import sys
import importlib
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

versions = []
package_root = os.path.dirname( os.path.abspath( __file__ ) )
files = [ f for f in os.listdir(package_root) if f[-3:]==".py" and f not in [ "__init__.py" ] ]

# print( f'Versions: __file__ = {__file__}, package_root = {package_root}' )
# print( 'Versions: files = ' + ", ".join( files ) )
# sys.stdout.flush()

for file in files: 
    modname = file[:-3]
    m = importlib.import_module( ".Versions." + modname, "rorefcat" )
    versions.append( { 'AWSversion': m.AWSversion, 'module': m,
            'level1b': m.format_level1b, 'level2a': m.format_level2a,
            'level2b': m.format_level2b } )

valid_versions = [ version['AWSversion'] for version in versions ]


################################################################################
#  Utilities
################################################################################

def get_version( AWSversion ):
    """Given a string AWSversion, return the element of the Versions.versions
    list corresponding to that AWS version. If one is not found, return None."""

    vs = [ version for version in versions if version['AWSversion'] == AWSversion ]

    if len( vs ) == 0:
        ret = None
    else:
        ret = vs[0]

    return ret

