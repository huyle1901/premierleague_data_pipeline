from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json

POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner":"airflow",
    "start_date":days_ago(1)
}

teams = [
    {"name":"Liverpool","url":"https://fbref.com/en/squads/822bd0ba/Liverpool-Stats"},
    {"name":"Arsenal","url":"https://fbref.com/en/squads/18bb7c10/Arsenal-Stats"},
    {"name":"Nottingham Forest","url":"https://fbref.com/en/squads/e4a775cb/Nottingham-Forest-Stats"},
    {"name":"Chelsea","url":"https://fbref.com/en/squads/cff3d9bb/Chelsea-Stats"},
    {"name":"Manchester City","url":"https://fbref.com/en/squads/b8fd03ef/Manchester-City-Stats"},
    {"name":"Newcastle United","url":"https://fbref.com/en/squads/b2b47a98/Newcastle-United-Stats"},
    {"name":"Bournemouth","url":"https://fbref.com/en/squads/4ba7cbea/Bournemouth-Stats"},
    {"name":"Aston Villa","url":"https://fbref.com/en/squads/8602292d/Aston-Villa-Stats"},
    {"name":"Brighton & Hove Albion","url":"https://fbref.com/en/squads/d07537b9/Brighton-and-Hove-Albion-Stats"},
    {"name":"Fulham","url":"https://fbref.com/en/squads/fd962109/Fulham-Stats"},
    {"name":"Brentford","url":"https://fbref.com/en/squads/cd051869/Brentford-Stats"},
    {"name":"Crystal Palace","url":"https://fbref.com/en/squads/47c64c55/Crystal-Palace-Stats"},
    {"name":"Manchester United","url":"https://fbref.com/en/squads/19538871/Manchester-United-Stats"},
    {"name":"West Ham United","url":"https://fbref.com/en/squads/7c21e445/West-Ham-United-Stats"},
    {"name":"Tottenham Hotspur","url":"https://fbref.com/en/squads/361ca564/Tottenham-Hotspur-Stats"},
    {"name":"Everton","url":"https://fbref.com/en/squads/d3fd31cc/Everton-Stats"},
    {"name":"Wolverhampton Wanderers","url":"https://fbref.com/en/squads/8cec06e1/Wolverhampton-Wanderers-Stats"},
    {"name":"Ipswich Town","url":"https://fbref.com/en/squads/b74092de/Ipswich-Town-Stats"},
    {"name":"Leicester City","url":"https://fbref.com/en/squads/a2d435b3/Leicester-City-Stats"},
    {"name":"Southampton","url":"https://fbref.com/en/squads/33c895d4/Southampton-Stats"}
]

# DAG
with DAG(dag_id = "player_stats_etl_pipeline",
         default_args = default_args,
         schedule_interval="@daily",
         catchup=False) as dags:
    @task()
    def extract_player_stats_data():
        """Extract stats of player from Web Using Beautiful Soup."""
        players_data = []

        for team in teams:
            print(f"Scraping data for {team["name"]}...")

            # Send a GET request to fetch the page content
            response = requests.get(team['url'])
            response.raise_for_status()

            # Parse the page content with BeautifulSoup
            soup = BeautifulSoup(response.text,"html.parser")

            # Locate the tbody containing the data
            tbody = soup.find("tbody")

            # Iterate over each row in the tbody
            for row in tbody.find_all("tr"):
                player_data = {'team': team["name"]}

                # Extract the player information from the 'th' tag
                th = row.find("th", {"data-stat": 'player'})
                if th:
                    player_name = th.text.strip()
                    player_url = th.find("a")["href"] if th.find("a") else None
                    player_data["player_name"] = player_name
                    player_data['player_url'] = f"https://fbref.com{player_url}" if player_url else None
                
                # Extract all "td" data
                for td in row.find_all("td"):
                    data_stat = td.get("data-stat")
                    if data_stat:
                        player_data[data_stat] = td.text.strip()
                # Add the player's data to the list only if "MP" > 0
                if player_data.get("games") and player_data["games"].isdigit() and int(player_data["games"]) >0:
                    players_data.append(player_data)
        return players_data
    @task()
    def transform_players_data(players_data):
        """ Transform the Scraping Data """
        transformed_players_data = []

        for player in players_data:
            filterd_player_data = {
                "team": player.get("team"),
                "player_name": player.get("player_name"),
                "url": player.get("player_url"),
                "nationality": player.get("nationality","")[-3:],
                "position": player.get("position"),
                "age": player.get("age"),
                "games": player.get("games")
            }
            transformed_players_data.append(filterd_player_data)
        return transformed_players_data
    @task()
    def load_player_data(transformed_players_data):
        """ Load transformed data into PostgresSQL"""
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_data (
            team TEXT,
            player_name TEXT,
            url TEXT,
            nationality TEXT,
            position TEXT,
            age TEXT,
            games INT    
            );
            """)
        
        # Insert transformed data into the table
        insert_query = """
        INSERT INTO player_data (team, player_name, url, nationality, position, age, games)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        for player in transformed_players_data:
            cursor.execute(insert_query, (
                player.get("team"),
                player.get("player_name"),
                player.get("url"),
                player.get("nationality"),
                player.get("position"),
                player.get("age"),
                int(player.get("games", 0)) if player.get("games") and player.get("games").isdigit() else None
        ))

        conn.commit()
        cursor.close()
    # DAG Workflow - ETL Pipeline
    players_data = extract_player_stats_data()
    transformed_players_data = transform_players_data(players_data)
    load_player_data(transformed_players_data)
        

        




            
        




        

