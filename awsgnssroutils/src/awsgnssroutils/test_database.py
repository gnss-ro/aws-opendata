import database

# database.populate()
db = database.RODatabaseClient()
occs = db.query( missions="cosmic2", datetimerange=("2022-03-03","2022-03-04") )
paths = occs[:100].download( "ucar_refractivityRetrieval" )

pass

