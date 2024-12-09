"""This package contains the following exports:

get_receiver_satellites
==============================
Given mission and receiver names for any processing center, return a
list of receiver satellites, each element of which is a dictionary. Each
dictionary has processing centers as keys whose corresponding values are
dictionaries with keys 'mission' and 'receiver' whose values in turn are
the mission and receiver names as defined for that processing center.
For example, sat['ucar'] = { 'mission': "cosmic1", 'receiver': "cosmic1c2" }
is one possibility.

receiversignals
==============================
Given transmitter and receiver names and a time, this function returns
a dictionary defining the signals tracked by the transmitter-receiver pair
at this time.

valid_missions
==============================
A dictionary whose keys are processing_centers names with corresponding
values that are lists of the names of the missions defined for the
processing_center.

"""

import os
import json
import importlib

#  Logger.

import logging
LOGGER = logging.getLogger( __name__ )


################################################################################
#  Initialize: Import all missions and their receiver satellites defined in
#  the "Missions" package.
################################################################################

modules = {}
receiver_satellites = []
package_root = os.path.dirname( __file__ )
files = [ f for f in os.listdir(package_root) if f[-3:]==".py" and f not in [ "template.py", "__init__.py" ] ]

for file in files: 
    modname = file[:-3]
    m = importlib.import_module( ".Missions." + modname, "rorefcat" )
    LOGGER.debug( f"modname={modname}, receivers=" + ",".join( [ sat['aws']['receiver'] for sat in m.satellites ] ) )
    receiver_satellites += m.satellites
    modules.update( { modname: m } )

LOGGER.debug( "Mission modules imported: " + ", ".join( sorted( list( modules ) ) ) )
LOGGER.debug( "Receiver satellites: " + ", ".join( sorted( [ sat['aws']['receiver'] for sat in receiver_satellites ] ) ) )

#  Generate a list of valid missions according to processing center.

valid_missions = {}
for sat in receiver_satellites:
    for center in sat.keys():
        if center in [ "signals", "wmo" ]: continue
        if center not in valid_missions.keys():
            valid_missions.update( { center: [] } )
        valid_missions[center].append( sat[center]['mission'] )

for center, value in valid_missions.items():
    valid_missions[center] = sorted( list( set( value ) ) )

LOGGER.debug( "valid_missions = " + json.dumps( valid_missions ) )


################################################################################
#  Utility functions.
################################################################################

def get_receiver_satellites( processing_center, mission=None, receiver=None ):
    """Retrieve a list of receiver satellites from the missions.Table according to the
    mission name and/or receiver name. The naming convention for each is taken
    for the named processing_center."""

    #  Leaf over satellites.

    sats = []
    for sat in receiver_satellites:
        keep = True
        if processing_center in sat.keys():
            if mission is not None:
                keep = keep & ( sat[processing_center]['mission'] == mission )
            if receiver is not None:
                keep = keep & ( sat[processing_center]['receiver'] == receiver )
        else:
            keep = False
        if keep:
            sats.append( sat )

    return sats


def receiversignals( transmitter, receiver, time, processing_center="aws" ):
    """Given a transmitter in the form of a RINEX3 GNSS PRN, a receiver
    name according to the naming convention of processing_center, and
    a class datetime time, return a list of dictionaries, each element
    corresponding to a signal with key standardName, key rinex3name,
    key loop, and key data. The possible values of standardName are
    "C/A", "L1", "L2". The rinex3name returns the RINEX-3 observation
    code for the carrier phase signal. The loop indicated whether it
    was tracked by closed loop ("closed") or open loop ("open"). the
    key data is a boolean indicating whether it is a data tone (with
    navigation message) or not. If no valid translation is found,
    return a None."""

    receiver_sats = get_receiver_satellites( processing_center, receiver=receiver )

    if len( receiver_sats ) != 1:
        raise missionsError( "InvalidArguments", f'Number of receivers found ({len(receiver_sats)} must be 1' )
    else:
        receiver_sat = receiver_sats[0]

    ret = receiver_sat['signals']( transmitter, receiver_sat['aws']['receiver'], time )

    return ret
