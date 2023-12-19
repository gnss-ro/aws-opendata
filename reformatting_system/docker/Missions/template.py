#  This is a template to be used for defining a new mission. Instructions 
#  are inter-lineated below. 

######################################################################
#  Useful imports. 
######################################################################

import re
from GNSSsatellites import get_transmitter_satellite

######################################################################
#  Exception handling. Leave intact. 
######################################################################

class Error( Exception ): 
    pass

class missionsError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment

######################################################################
#  Required: variable "mission"
######################################################################
#
#  This is the name of the mission as published in the AWS Registry of 
#  Open Data. It *must* match the name of this module. 

mission = "new_mission"

######################################################################
#  Required: function "signals"
######################################################################
#
#  This function is required, it must have the name "signals", 
#  and it must take transmitter, receiver, and time as its 
#  mandatory arguments. 
#
#   transmitter         The name of the GNSS transmitter tracked, 
#                       as a three-character string, the first 
#                       character a capital letter defining the 
#                       GNSS constellation, and the second and 
#                       third characters a zero-padded integer 
#                       defining the PRN identifier of the satellite. 
#
#   receiver            The name of the RO receiver satellite, 
#                       according to the AWS naming conventions. 
#                       It is generally not used in evaluating the 
#                       function, but it potentially could be for 
#                       future scenarios, especially next-generation 
#                       Metop/GRAS. 
#
#   time                A datetime.datetime class definition of time 
#                       of the occultation. 
#
#  The function returns a list of dictionaries, each element 
#  of the list being a dictionary defining a GNSS signal being 
#  tracked at high-rate to record RO information. The keys-values 
#  of each dictionary are 
#
#   'standardName' ->   A mnemonic descriptor of the signal being 
#                       tracked, such as "C/A", "L1", "L2", etc. 
#                       It exists for user friendliness. 
#
#   'rinex3name' ->     The RINEX-3 observation code of the tracked 
#                       phase. It is a three-character string 
#                       always beginning with "L". See "observation
#                       codes" for carrier phase in tables 4 through 
#                       10 in RINEX 3 documentation. 
#
#   'loop' ->           Either "closed" or "open" depending on whether 
#                       the signal is tracked in closed or open 
#                       loop.  The tracking should be designated as 
#                       open loop if any part of the occultation is 
#                       tracked in open loop. 

def signals( transmitter, receiver, time ): 
    """Given a transmitter in the form of a RINEX3 GNSS PRN, a 
    receiver name, and a class datetime time, return a list of 
    dictionaries, each element corresponding to a signal with key 
    standardName, key rinex3name, key loop, and key data. The 
    possible values of standardName are "C/A", "L1", "L2". The 
    rinex3name returns the RINEX-3 observation code for the carrier 
    phase signal. The loop indicated whether it was tracked by 
    closed loop ("closed") or open loop ("open"). """

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



############################################################
#  Required: list variable "satellites"
############################################################
#  
#  It is a list of dictionaries, each element of which defines 
#  the receiver satellites for this mission: the AWS naming 
#  convention, the naming convention for each contributing 
#  RO processing center. Separately, it also defines the WMO 
#  identifier for each receiver satellite. 
#
#  The key-value pairs of each dictionary element are 
#
#  'signals' -> which points to the function "signals" defined 
#               herein. 
#
#   processing center -> which points to another dictionary 
#               containing the keys 'mission' and 'receiver', 
#               which in turn are the string names of the 
#               mission and receiver associated with the 
#               satellite for this processing center. Any 
#               number of these can be included according to 
#               the number of processing centers that process 
#               data for this mission. 
#
#   'wmo' -> which points to a dictionary with just one key-
#               value pair. That pair is 'id', which points to 
#               an integer defining the WMO ID of this receiver. 
#               If not WMO ID has been defined yet, then do not 
#               define the 'wmo' entry at all. 
#

satellites = []

#  Six receiver satellites in this case. 

for i in range(6): 

    #  Leave intact! 

    rec = { 'signals': signals }

    #  Processing center entries. 

    rec.update( { 'aws': { 'mission': mission, 'receiver': f"cosmic2e{i+1:1d}" } } )        #  AWS convention
    rec.update( { 'jpl': { 'mission': mission, 'receiver': f"cosmic2e{i+1:1d}" } } )        #  JPL convention
    rec.update( { 'ucar': { 'mission': "cosmic2", 'receiver': f"C2E{i+1:1d}" } } )          #  UCAR convention

    #  WMO ID definitions. 

    rec.update( { 'wmo': { 'satellite_id': None,  'satellite_subid': None, 
                          'instrument_id': None } } )

    #  Append rec to satellites list. 

    satellites.append( rec )

######################################################################
#  Done. 
######################################################################

