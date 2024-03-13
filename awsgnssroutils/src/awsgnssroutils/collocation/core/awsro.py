from awsgnssroutils.database import OccList
from datetime import datetime 
from .timestandards import Time

awsro_time_convention = "utc"

#  Exception handling. 

class Error( Exception ): 
    pass

class awsroError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment


def get_occ_times( occs ): 
    """Generate a list of instance of timestandards.Time of the occultations in occs. 
    occs must be an instance of OccList."""

    if not isinstance(occs,OccList): 
        raise awsroError( "InvalidArgument", "Argument must be an instance of OccList" )

    datetimes = occs.values( "datetime" )
    ret = [ Time( **{ awsro_time_convention: datetime.strptime( t, "%Y-%m-%d-%H-%M" ) } ) for t in datetimes ]

    #  Done. 

    return ret

