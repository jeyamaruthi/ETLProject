import json
import pandas as pd
import requests

import psycopg2


# function to extract data from the API and convert it into a DataFrame
def get_data():
    url = r"https://official-joke-api.appspot.com/random_ten"
    response = requests.get(url)
    data = json.loads(response.text)

    # this normalizes the semi-structured data and turns it into a dataframe
    df = pd.json_normalize(data=data)
    return df


# Defining a function to save the data into a PostgreSQL database
def commit_to_postgres():
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres",
                            password="1967", port=5432)

    cur = conn.cursor()

    # sql syntax to create the table that would hold our data
    create_table_query = """ 
        CREATE TABLE jokes_data(
                    type text,
                    setup text,
                    punchline text,
                    id integer primary key
                    ) ;
                """

    cur.execute(create_table_query)

    df_joke_data = get_data()

    for _, row in df_joke_data.iterrows():
        cur.execute(
            "INSERT INTO jokes_data (id, type, setup, punchline) VALUES (%s, %s, %s, %s)",
            (
                row["id"],
                row["type"],
                row["setup"],
                row["punchline"]),
        )

    conn.commit()

    cur.close()

    conn.close()

get_data()

commit_to_postgres()