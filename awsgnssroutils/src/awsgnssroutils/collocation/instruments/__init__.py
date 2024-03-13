instruments = {}

from .jpss_atms import JPSS_ATMS, instrument, valid_satellites
instruments.update( { instrument: { 'class': JPSS_ATMS, 'valid_satellites': valid_satellites } } )

from .metop_amsua import Metop_AMSUA, instrument, valid_satellites
instruments.update( { instrument: { 'class': Metop_AMSUA, 'valid_satellites': valid_satellites } } )

satellites = {}
for instrument, value in instruments.items(): 
    for satellite in value['valid_satellites']: 
        if satellite not in satellites.keys(): 
            satellites.update( { satellite: [] } )
        satellites[satellite].append( instrument )

