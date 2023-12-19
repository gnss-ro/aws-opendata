import re
from datetime import datetime, timedelta
import numpy as np

def fix( longitude, time ):
    """Generate a local time given longitude and time. The time can be either
    the "time" metadata variable or the date-time sort key."""

    try:
        t = datetime.datetime.fromisoformat( time )
    except:
        t = None

    if t is None: 
        m = re.search( "^(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})$", time )
        if m:
            year, month, day = int( m.group(1) ), int( m.group(2) ), int( m.group(3) )
            hour, minute = int( m.group(4) ), int( m.group(5) )
            t = datetime.datetime( year=year, month=month, day=day, hour=hour, minute=minute )

    if t is None:
        print( "Invalid argument time" )
        return None

    dt = np.deg2rad( ( t.hour + t.minute/60.0 ) * 15 + longitude )
    local_time = np.rad2deg( np.arctan2( -np.sin(dt), -np.cos(dt) ) ) / 15 + 12

    return local_time

if __name__ == "__main__":
    import pdb
    pdb.set_trace()
    lt1 = fix( 30.5, "2009-09-12-20-08" )
    lt2 = fix( 30.5, "2009-09-12T20:08:00" )
    print( lt1, lt2 )
    pass
