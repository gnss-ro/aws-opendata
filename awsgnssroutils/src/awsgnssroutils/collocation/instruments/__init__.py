import importlib.metadata
__version__ = importlib.metadata.version("awsgnssroutils")

instruments = {}

from .atms import ATMS, instrument, valid_satellites
instruments.update( { instrument: { 'class': ATMS, 'valid_satellites': valid_satellites } } )

from .amsua import AMSUA, instrument, valid_satellites
instruments.update( { instrument: { 'class': AMSUA, 'valid_satellites': valid_satellites } } )

from .airs import AIRS, instrument, valid_satellites
instruments.update( { instrument: { 'class': AIRS, 'valid_satellites': valid_satellites } } )

satellites = {}
for instrument, value in instruments.items(): 
    for satellite in value['valid_satellites']: 
        if satellite not in satellites.keys(): 
            satellites.update( { satellite: [] } )
        satellites[satellite].append( instrument )

