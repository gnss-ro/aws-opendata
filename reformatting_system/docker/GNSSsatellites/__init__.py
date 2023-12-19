"""This package contains information on GNSS satellite history. Its 
exports are: 

get_transmitter_satellite
==============================
This function returns the definition of a GNSS transmitter satellite 
given a PRN and a time. 

carrierfrequency
==============================
Return the carrier frequency broadcast by a transmitter satellite at 
a specified time. 

valid_transmitters
==============================
This is a list of valid transmitter names, each a 3-character RINEX-3 
string, such as "G03", etc. 

valid_sensors
==============================
This is a list containing valid sensor/block names for all GNSS 
constellations. 

"""

import os
import re
import requests
from datetime import datetime

#  Exception handling. 

class Error( Exception ):
    pass

class GNSSsatellitesError( Error ):
    def __init__( self, message, comment ):
        self.message = message
        self.comment = comment


################################################################################
#  Read satellite history file. 
################################################################################

def Read_Bernese_GNSS_Satellites( satellitefile ): 
    """This function creates a Table_GNSS_constellations from a satellite 
    history file as provided by the University of Bern."""

    if not os.path.exists( satellitefile ): 
        comment = f'Satellite file "{satellitefile}" does not exist'
        LOGGER.error( comment )
        return None

    #  Open file and retain lines of the "ON-BOARD SENSORS" section only. 

    with open( satellitefile, 'r' ) as s: 
        alllines = s.readlines()

    lines = []
    headerline = "PART 2: ON-BOARD SENSORS"

    while len( alllines ) > 0: 
        line = alllines.pop(0).rstrip()
        if line[:len(headerline)] == headerline: 
            break

    while len( alllines ) > 0: 
        line = alllines[0].rstrip()
        if re.search( "^-*$", line ) or re.search( "START TIME", line ) \
                or re.search( "^PRN", line ) or line == "": 
            alllines.pop(0)
            continue
        else: 
            break

    while len( alllines ) > 0: 
        line = alllines.pop(0).rstrip()
        if line == "": 
            break
        else: 
            lines.append( line )

    #  Initialize constellations. 

    GPS = { 'code': "G", 'name': "GPS", 'list': [] }
    GLONASS = { 'code': "R", 'name': "GLONASS", 'list': [] }
    GALILEO = { 'code': "E", 'name': "GALILEO", 'list': [] }
    BeiDou = { 'code': "C", 'name': "BeiDou", 'list': [] }
    QZSS = { 'code': "J", 'name': "QZSS", 'list': [] }

    Table = ( GPS, GLONASS, GALILEO, BeiDou, QZSS )

    #  Parse the sensors data lines. 

    for line in lines: 

        if line[5:9].rstrip() != "MW": continue

        prn = int( line[0:3] )
        sensor_name = line[11:28].strip()
        svn = int( line[28:31] )
        number = int( line[33:39] )
        antex_sensor_name = line[171:191].strip()
        ifrq = int( line[193:197] )

        if line[41:60].strip() == "": 
            start_time = None
        else: 
            start_time = datetime( year=int(line[41:45]), month=int(line[46:48]), day=int(line[49:51]), 
                            hour=int(line[52:54]), minute=int(line[55:57]), second=int(line[58:60]) )

        if line[62:81].strip() == "": 
            end_time = None
        else: 
            end_time = datetime( year=int(line[62:66]), month=int(line[67:69]), day=int(line[70:72]), 
                            hour=int(line[73:75]), minute=int(line[76:78]), second=int(line[79:81]) )

        if antex_sensor_name == "": 
            comment = 'No antex sensor provided'
            LOGGER.error( comment )
            return None

        if prn > 0 and prn < 100: 
            constellation = GPS
        elif prn > 100 and prn < 200: 
            constellation = GLONASS
        elif prn > 200 and prn < 300: 
            constellation = GALILEO
        elif prn > 400 and prn < 500: 
            constellation = BeiDou
        elif prn > 500 and prn < 600: 
            constellation = QZSS
        else: 
            continue

        #  Add a satellite to the "constellation".  

        rec = { 'prn': prn % 100, 'svn': svn, 'sensor': antex_sensor_name, 'channel': ifrq, 
               'start_time': start_time, 'end_time': end_time }
        constellation['list'].append( rec )

    #================================================================================
    #  Carrier frequencies. The carrier frequencies are defined below for each 
    #  GNSS accorrding to the first two characters of the Rinex-3 carrier phase 
    #  observation code. 
    #================================================================================

    #  Define carrier frequencies for GPS satellites. 

    for sat in GPS['list']: 
        sat.update( { 'frequencies': { 
            'L1': 154 * 10.23e6, 
            'L2': 120 * 10.23e6, 
            'L5': 115 * 10.23e6 } } )

    #  Define carrier frequencies for GLONASS satellites. 

    for sat in GLONASS['list']: 
        sat.update( { 'frequencies': { 
            'L1': 1602.0e6 + 0.5625e6 * sat['channel'],
            'L2': 1246.0e6 + 0.4375e6 * sat['channel'],
            'L3': 1201.0e6 + 0.4375e6 * sat['channel'],
            'L4': 1600.995e6, 
            'L6': 1248.06e6 } } )

    #  Define carrier frequencies for Galileo satellites.

    for sat in GALILEO['list']: 
        sat.update( { 'frequencies': {
            'L1': 1575.42e6, 
            'L5': 1176.45e6, 
            'L6': 1278.75e6,
            'L7': 1207.14e6, 
            'L8': 1191.795e6 } } )

    #  Define carrier frequencies for BeiDou satellites.

    for sat in BeiDou['list']: 
        sat.update( { 'frequencies': {
            'L1': 1575.42e6, 
            'L2': 1561.098e6, 
            'L5': 1176.45e6, 
            'L6': 1268.52e6, 
            'L7': 1207.140e6, 
            'L8': 1191.795e6 } } )

    #  Define carrier frequencies for QZSS satellites. 

    for sat in QZSS['list']: 
        sat.update( { 'frequencies': { 
            'L1': 1575.42e6, 
            'L2': 1227.60e6, 
            'L5': 1176.45e6, 
            'L6': 1278.75e6 } } )

    #===============================================================================
    #  Define which GNSS signals carry data, usually navigation messages. 
    #===============================================================================

    for sat in GPS['list']: 
        sat.update( { 'data_tones': [ "L1C", "L2C", "L5I" ]  } )

    for sat in GLONASS['list']: 
        sat.update( { 'data_tones': [ "L1C", "L2C" ]  } )

    for sat in GALILEO['list']: 
        sat.update( { 'data_tones': [
            "L1A", "L1B", "L1X", "L1Z", 
            "L5I", "L5X", "L7I", "L7X", 
            "L8I", "L8Q", "L8X", 
            "L6A", "L6B", "L6X", "L6Z" ] } )

    for sat in BeiDou['list']: 
        sat.update( { 'data_tones': [
            "L1D", "L1X", "L5D", "L5X", 
            "L7D", "L7Z", "L8D", "L8X" ] } )

    for sat in QZSS['list']: 
        sat.update( { 'data_tones': [ "L1C" ] } )

    #  Done. 

    return Table


################################################################################
#  Initialize. Retrieve satellite data if possible. 
################################################################################

try: 
    r = requests.get( "http://ftp.aiub.unibe.ch/BSWUSER54/CONFIG/SATELLIT_I20.SAT" )
    if r.status_code == 200: 
        tmpfile = "satellitehistory.dat"
        f = open( tmpfile, "w" )
        f.write( r.text )
        f.close()
        Table = Read_Bernese_GNSS_Satellites( tmpfile )
        os.unlink( tmpfile )
    else: 
        Table = None
except:
    Table = None

#  If downloading data is impossible, use local satellite history file. 

if Table is None: 
    satellite_history_file = os.getenv( "SATELLITEHISTORY" )
    if satellite_history_file is not None: 
        Table = Read_Bernese_GNSS_Satellites( satellite_history_file )

if Table is None: 
    raise GNSSsatellitesError( "FileNotFound", "Could not find satellite history " + \
            "file, or environment variable SATELLITEHISTORY is not set" )

#  Generate a list of valid satellites. 

valid_transmitters = [ f"{constellation['code']}{sat['prn']:02d}" 
              for constellation in Table 
              for sat in constellation['list'] ]

#  Generate a list of valid sensors. 

valid_sensors = sorted( list( { sat['sensor'] for const in Table for sat in const['list'] } ) )


################################################################################
#  Utilitity functions. 
################################################################################

def get_transmitter_satellite( prn, time ): 
    """Get a satellite from the GNSS satellite table. The prn is a 3-character 
    string giving the satellite PRN, as in "G03". The time is an instance of 
    datetime.datetime defining a reference time."""

    constellation = prn[0]
    iprn = int( prn[1:] )

    sats = []
    for const in Table: 
        if const['code'] == constellation: 
            sats = const['list']

    satellite = None
    for sat in sats: 
        if sat['prn'] != iprn or time < sat['start_time']: 
            continue
        if sat['end_time'] is None: 
            satellite = sat
            break
        elif time < sat['end_time']: 
            satellite = sat
            break

    return satellite


def carrierfrequency( prn, date, obsCode ):
    """Return the carrier frequency (Hz) given a GNSS satellite prn, a date (class 
    datetime.datetime), and an observation code (obsCode). The prn should be the 
    three-digit RINEX standard wherein the first letter identifies the system ("G", 
    "R", etc.) and the second and third are digits identify the PRN. The function 
    raises an exception if it is unable to determine a carrier frequency."""

#  Get the constellation code and the satellite PRN identifier.

    satellite = get_transmitter_satellite( prn, date )

    if satellite is None: 
        raise GNSSsatellitesError( "UnrecognizedSatellite", f'Satellite "{prn}" is unrecognized.' )

    obs = "L" + obsCode[1]
    out = satellite['frequencies'][obs]

    return out 


