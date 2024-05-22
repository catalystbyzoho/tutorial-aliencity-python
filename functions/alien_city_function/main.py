import json
import zcatalyst_sdk  
from flask import Request, make_response, jsonify  
import logging
tableName = 'AlienCity'  # The table created in the Data Store
columnName = 'CityName'  # The column created in the table
def handler(request: Request):
    try:
        app = zcatalyst_sdk.initialize()    # Initializing Catalyst SDK
        logger = logging.getLogger()
        if request.path == "/alien" and request.method == 'POST':   # The POST API that reports the alien encounter for a particular city
            req_data = request.get_json()
            name = req_data.get('city_name')
            rowid = getAlienCountFromCatalystDataStore(name)    # Queries the Catalyst Data Store table and checks whether a row is present for the given city
            if len(rowid) == 0:         # If the row is not present, then a new row is inserted
                logger.info("Alien alert!")     # Written to the logs. You can view this log from Logs under the Monitor section in the console 
                datastore_service = app.datastore()
                table_service = datastore_service.table(tableName)
                row_data = {
                    columnName:name
                    }
                table_service.insert_row(row_data)  # Inserts the city name as a row in the Catalyst Data Store table
                response = make_response(jsonify({
                "message": "Thanks for reporting!"
            }), 200)
            else:       # If the row is present, then a message is sent indicating duplication
                response = make_response(jsonify({
                "message": "Looks like you are not the first person to encounter aliens in this city! Someone has already reported an alien encounter here!"
            }), 200)
            return response
        elif request.path == "/alien" and request.method == 'GET':  # The GET API that checks the table for an alien encounter in that city 
            name = request.args.get('city_name')
            rowid = getAlienCountFromCatalystDataStore(name)    # Queries the Catalyst Data Store table and checks whether a row is present for the given city
            if len(rowid) == 0:
                response = make_response({
                    "message": "Hurray! No alien encounters in this city yet!",
                    "signal": "negative"
                }, 200)
            else:
                response = make_response(jsonify({
                "message":  "Uh oh! Looks like there are aliens in this city!",
                "signal": "positive"
            }), 200)
            return response    
        else:
            response = make_response("Error. Invalid Request")
            response.status_code = 404
            return response
    except Exception as err:    # Sends an error response
        logger.error(f"Exception in AlienCityAIO :{err}")
        response = make_response(jsonify({
                 "error": "Internal server error occurred. Please try again in some time."
            }), 500)
        return response
def getAlienCountFromCatalystDataStore(cityname):   # Checks whether an alien encounter is already reported for the given city by querying the Data Store table
        app = zcatalyst_sdk.initialize()   
        zcql_service = app.zcql()
        query = f"SELECT * FROM {tableName} WHERE {columnName} = {cityname}"
        output = zcql_service.execute_query(query)
        return output