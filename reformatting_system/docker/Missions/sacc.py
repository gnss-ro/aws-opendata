import re
from GNSSsatellites import get_transmitter_satellite

#  Exception handling. 

class Error( Exception ): 
    pass

class missionsError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment

#  Parameters. 

mission = "sacc"

#  Define the signals tracked by the mission's satellites. 

def signals( transmitter, receiver, time ): 
    """Given a transmitter in the form of a RINEX3 GNSS PRN, a 
    receiver name, and a class datetime time, return a list of 
    dictionaries, each element corresponding to a signal with key 
    standardName, key rinex3name, key loop, and key data. The 
    possible values of standardName are "C/A", "L1", "L2". The 
    rinex3name returns the RINEX-3 observation code for the carrier 
    phase signal. The loop indicated whether it was tracked by 
    closed loop ("closed") or open loop ("open"). the key data is a 
    boolean indicating whether it is a data tone (with navigation 
    message) or not. If no valid translation is found, return a 
    None."""

    satellite = get_transmitter_satellite( transmitter, time )
    constellation = transmitter[0]
    ret = None

    #  GPS. 

    if constellation == "G": 
        m = re.search( "^BLOCK (\S+)", satellite['sensor'] )
        if m: 
            block = m.group(1)
        else: 
            raise missionsError( "ParseError", 'Unable to parse GNSS satellites sensor "{:}".'.format( satellite['sensor'] ) )

        ret = [ { 'standardName': "C/A", 'rinex3name': "L1C", 'loop': "open" }, 
                   { 'standardName': "L2", 'rinex3name': "L2W", 'loop': "open" } ]

    else: 
        raise missionsError( "UndefinedSignals", 'No signals defined for constellation ' + \
                f'ID "{constellation}" for receiver "{receiver}".' )

    return ret


#  Define the list of satellites in the mission. 

satellites = []

for receiver in [ "sacc" ]: 
    rec = { 'signals': signals }
    rec.update( { 'aws': { 'mission': mission, 'receiver': receiver } } )
    rec.update( { 'jpl': { 'mission': mission, 'receiver': receiver } } )
    rec.update( { 'ucar': { 'mission': "sacc", 'receiver': "SACC" } } )
    rec.update( { 'wmo': { 'satellite_id': 820, 'instrument_id': 102 } } )
    satellites.append( rec )

