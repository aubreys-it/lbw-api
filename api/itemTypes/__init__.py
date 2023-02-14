import logging
import os
import pyodbc
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    DMCP_CONNECT_STRING = os.environ['DMCP_CONNECT_STRING']
    
    if req.method == "GET":
        logging.info("Processing GET Request.")

        sql = "SELECT * FROM lbw.item_types;"

        conn = pyodbc.connect(DMCP_CONNECT_STRING)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        itemTypes = {}
        for row in rows:
            if not row[0] in itemTypes.keys():
                itemTypes[row[0]] = row[1]

        if itemTypes:
            return func.HttpResponse(json.dumps(itemTypes), status_code=200)

    elif req.method == "POST":

        logging.info("Processing POST Request.")

        itemName = req.params.get('itemName')
        if not itemName:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                itemName = req_body.get('itemName')

        if itemName:
            itemName = itemName.replace("'", "''")
            sql = f"INSERT INTO lbw.item_types (item_type) VALUES ('{itemName}');"

            conn = pyodbc.connect(DMCP_CONNECT_STRING)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            
            cursor.close()
            conn.close()

            return func.HttpResponse(
                "Success",
                status_code=200
            )

    elif req.method == "PATCH":

        logging.info("Processing Patch Request.")

        itemId = req.params.get('itemId')
        itemName = req.params.get('itemName')

        if not itemId and itemName:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                itemId = req_body.get('itemId')
                itemName = req_body.get('itemName')

        if itemId and itemName:
            itemName = itemName.replace("'", "''")
            sql = f"UPDATE lbw.item_types SET item_type='{ itemName }' WHERE item_type_id={ itemId };"

            conn = pyodbc.connect(DMCP_CONNECT_STRING)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            
            cursor.close()
            conn.close()

            return func.HttpResponse(
                "Success",
                status_code=200
            )

    elif req.method == "DELETE":

        logging.info("Processing Delete Request.")

        itemId = req.params.get('itemId')

        if not itemId:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                itemId = req_body.get('itemId')

        if itemId:
            sql = f"DELETE FROM lbw.item_types WHERE item_type_id={ itemId };"

            conn = pyodbc.connect(DMCP_CONNECT_STRING)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            
            cursor.close()
            conn.close()

            return func.HttpResponse(
                "Success",
                status_code=200
            )
                