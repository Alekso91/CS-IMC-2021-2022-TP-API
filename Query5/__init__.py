import logging
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = req.params.get('genre')
    if not genre:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('genre')
    
    actor = req.params.get('actor')
    if not actor:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('actor')
    
    director = req.params.get('director')
    if not director:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('director')
    

    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""

    try:
        request_genre = f"SELECT t.tconst, runtimeMinutes\
            FROM [dbo].[tTitles] as t\
            INNER JOIN [dbo].[tGenres] g\
            ON t.tconst = g.tconst\
            WHERE t.tconst = g.tconst \
            AND genre = '{genre}'" if genre else None

        request_actor = f"SELECT t.tconst, runtimeMinutes\
            FROM [dbo].[tTitles] AS t\
            INNER JOIN [dbo].[tPrincipals] a\
            ON T.tconst = a.tconst\
            INNER JOIN [dbo].[tNames] b\
            ON b.nconst = a.nconst\
            WHERE category = 'acted in' AND primaryName = '{actor}'" if actor else None

        request_director = f"SELECT t.tconst, runtimeMinutes\
            FROM [dbo].[tTitles] AS t\
            INNER JOIN [dbo].[tPrincipals] a\
            ON T.tconst = a.tconst\
            INNER JOIN [dbo].[tNames] b\
            ON b.nconst = a.nconst\
            WHERE category = 'directed' AND primaryName = '{director}'" if director else None

        query = "SELECT AVG(runtimeMinutes) FROM [dbo].[tTitles]"

        if not request_genre and not request_actor and not request_director:
            query = "SELECT AVG(runtimeMinutes) FROM [dbo].[tTitles]"
        else:
            filters = ' INTERSECT '.join(filter(None, [request_genre, request_actor, request_director]))
            query = f"WITH request AS ( {filters} ) SELECT AVG(runtimeMinutes) FROM request"

        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()

            cursor.execute(query)

            rows = cursor.fetchall()
            for row in rows:
                dataString += f"SQL: average={row[0]}\n"


    except:
        errorMessage = "Erreur de connexion a la base SQL"
    
    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexion réussie et SQL!")
