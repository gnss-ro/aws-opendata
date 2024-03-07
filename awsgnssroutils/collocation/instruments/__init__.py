import pkgutil
# import inspect
import os

root = os.path.dirname( __file__ )

instruments = {}

for importer, modname, ispkg in pkgutil.walk_packages( [ root ] ): 
    m = importer.find_module( modname ).load_module( modname )
    instruments.update( { m.instrument_name: m.instrument } )

valid_instruments = sorted( list( instruments.keys() ) )

