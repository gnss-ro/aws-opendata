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

mission = "cosmic2"

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
                    { 'standardName': "L2", 'rinex3name': "L2L", 'loop': "open" } 
                  ]

        elif block in [ "I", "II", "IIA", "IIR-A", "IIR-B" ]: 
            ret = [ 
                    { 'standardName': "C/A", 'rinex3name': "L1C", 'loop': "open" },
                    { 'standardName': "L2", 'rinex3name': "L2W", 'loop': "open" } 
                  ]

    #  GLONASS. 

    elif constellation == "R": 
        ret = [ 
                { 'standardName': "C/A", 'rinex3name': "L1C", 'loop': "open" },
                { 'standardName': "L2", 'rinex3name': "L2C", 'loop': "open" } 
              ]

    else: 
        raise missionsError( "UndefinedSignals", f'No signals defined for constellation ID "{constellation}" for mission "{mission}".' )

    return ret


satellites = []

for i in range(6): 
    rec = { 'signals': signals }
    rec.update( { 'aws': { 'mission': mission, 'receiver': f"cosmic2e{i+1:1d}" } } )        #  AWS name
    rec.update( { 'jpl': { 'mission': mission, 'receiver': f"cosmic2e{i+1:1d}" } } )        #  JPL name
    rec.update( { 'ucar': { 'mission': "cosmic2", 'receiver': f"C2E{i+1:1d}" } } )          #  UCAR name
    rec.update( { 'wmo': { 'satellite_id': 750+i, 'instrument_id': 104 } } )                #  WMO ID
    satellites.append( rec )

