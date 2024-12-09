import re
from ..GNSSsatellites import get_transmitter_satellite

#  Exception handling.

class Error( Exception ):
    pass

class missionsError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment

#  Parameters.

mission = "metop"

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
        m = re.search( r"^BLOCK (\S+)", satellite['sensor'] )
        if m:
            block = m.group(1)
        else:
            raise missionsError( "ParseError", 'Unable to parse GNSS satellites sensor "{:}".'.format( satellite['sensor'] ) )

        ret = [ { 'standardName': "C/A", 'rinex3name': "L1C", 'loop': "open" },
                   { 'standardName': "L1", 'rinex3name': "L1W", 'loop': "open" },
                   { 'standardName': "L2", 'rinex3name': "L2W", 'loop': "open" } ]

    else:
        raise missionsError( "UndefinedSignals", 'No signals defined for constellation ' + \
                f'ID "{constellation}" for receiver "{receiver}".' )

    return ret


#  Define the list of satellites in the mission.

satellites = []

for ireceiver in range(3):
    letter = chr( ord('a') + ireceiver )
    rec = { 'signals': signals }

    #  AWS, JPL, UCAR, and ROMSAF conventions.

    rec.update( { 'aws': { 'mission': mission, 'receiver': f'metop{letter}' } } )
    rec.update( { 'jpl': { 'mission': mission, 'receiver': f'metop{letter}' } } )
    rec.update( { 'ucar': { 'mission': f'metop{letter}', 'receiver': f'MTP{letter.upper()}' } } )
    rec.update( { 'romsaf': { 'mission': "metop", 'receiver': f'MET{letter.upper()}' } } )

    #  EUMETSAT naming is a bit odd. Metop-A is "M02"; Metop-B is "M01"; and Metop-C is "M03"...

    if ireceiver == 0:
        rec.update( { 'eumetsat': { 'mission': f'metop{letter}', 'receiver': "M02" } } )
        rec.update( { 'wmo': { 'satellite_id': 4, 'instrument_id': 202 } } )
    elif ireceiver == 1:
        rec.update( { 'eumetsat': { 'mission': f'metop{letter}', 'receiver': "M01" } } )
        rec.update( { 'wmo': { 'satellite_id': 3, 'instrument_id': 202 } } )
    else:
        rec.update( { 'eumetsat': { 'mission': f'metop{letter}', 'receiver': f"M{ireceiver+1:02d}" } } )
        rec.update( { 'wmo': { 'satellite_id': 5, 'instrument_id': 202 } } )

    satellites.append( rec )
