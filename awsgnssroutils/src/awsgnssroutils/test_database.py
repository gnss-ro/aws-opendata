import database

db = database.RODatabaseClient()
occs = db.query( missions="cosmic2", datetimerange=("2022-03-03","2022-03-04") )

pass

