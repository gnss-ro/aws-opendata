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

mission = "cosmic1"

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

        if block in [ "IIR-M", "IIF", "IIIA" ]: 
            ret = [ 
                    { 'standardName': "C/A", 'rinex3name': "L1C", 'loop': "open" },
                    { 'standardName': "L1", 'rinex3name': "L1W", 'loop': "open" }, 
                    { 'standardName': "L2", 'rinex3name': "L2X", 'loop': "open" } 
                  ]

        elif block in [ "I", "II", "IIA", "IIR-A", "IIR-B" ]: 
            ret = [ 
                    { 'standardName': "C/A", 'rinex3name': "L1C", 'loop': "open" },
                    { 'standardName': "L1", 'rinex3name': "L1W", 'loop': "open" }, 
                    { 'standardName': "L2", 'rinex3name': "L2W", 'loop': "open" } 
                  ]

    else: 
        raise missionsError( "UndefinedSignals", f'No signals defined for constellation ID "{constellation}" for mission "{mission}".' )

    return ret


#  Define the list of satellites in the mission. 

satellites = []

for i in range(6): 
    rec = { 'signals': signals }
    rec.update( { 'aws': { 'mission': "cosmic1", 'receiver': f"cosmic1c{i+1:1d}" } } )      #  AWS name
    rec.update( { 'jpl': { 'mission': "cosmic1", 'receiver': f"cosmic1c{i+1:1d}" } } )      #  JPL name
    rec.update( { 'ucar': { 'mission': "cosmic1", 'receiver': f"C{i+1:03d}" } } )           #  UCAR name
    rec.update( { 'romsaf': { 'mission': "cosmic", 'receiver': f"C{i+1:03d}" } } )          #  ROMSAF name
    rec.update( { 'eumetsat': { 'mission': "cosmic1", 'receiver': f"C{i+1:02d}" } } )       #  EUMETSAT name
    rec.update( { 'wmo': { 'satellite_id': 740+i, 'instrument_id': 103 } } )                #  WMO ID
    satellites.append( rec )

