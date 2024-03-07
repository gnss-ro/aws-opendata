from awsgnssroutils.database import OccList
from collocation.core.TimeStandards import Time
from datetime import datetime 

awsro_time_convention = "utc"

#  Exception handling. 

class Error( Exception ): 
    pass

class awsroError( Error ): 
    def __init__( self, message, comment ): 
        self.message = message
        self.comment = comment


def get_occ_times( occs ): 
    """Generate a list of instance of TimeStandards.Time of the occultations in occs. 
    occs must be an instance of OccList."""

    if not isinstance(occs,OccList): 
        raise awsroError( "InvalidArgument", "Argument must be an instance of OccList" )

    datetimes = occs.values( "datetime" )
    ret = [ Time( **{ awsro_time_convention: datetime.strptime( t, "%Y-%m-%d-%H-%M" ) } ) for t in datetimes ]

    #  Done. 

    return ret

