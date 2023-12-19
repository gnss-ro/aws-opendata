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

versions = []
pp = os.getenv( "PYTHONPATH" )

for importer, modname, ispkg in pkgutil.walk_packages( [ os.path.join( pp, "Versions" ), "Versions" ] ):
    LOGGER.info( f"Importing Versions.{modname}" )
    m = importer.find_module(modname).load_module(modname)
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
