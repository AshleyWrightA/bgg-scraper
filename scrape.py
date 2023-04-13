# Standard Library Imports
import requests
import datetime as dt
import logging
import time

# Third Party Imports
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, InvalidName
from bs4 import BeautifulSoup
from re import sub

logger = logging.getLogger(__name__)


def get_date(num_days):
    return str(dt.date.today() - dt.timedelta(num_days+1))


def _fetch_bgg_page(date):
    bgg_url = f"https://boardgamegeek.com/geekplay.php?userid=0&startdate={date}&enddate={date}&action=bygame&subtype" \
              f"=All"
    page = requests.get(bgg_url)
    return page.text


def get_bgg_play_data(num_days):
    """Parses scraped data and returns a dict of play data"""
    for i in range(0, num_days):
        date = get_date(i)
        soup = BeautifulSoup(_fetch_bgg_page(date), "html.parser")
        write_bgg_date(soup.findAll("table", {"class": "forum_table"})[1].stripped_strings, date)
        time.sleep(1)


def write_bgg_date(table_data, date):
    db = get_database()
    clean_table = get_clean_data(table_data)
    boardgames_collection = db["boardGames"]
    plays_collection = db["plays"]

    # The first game in the table_data list is at index 4
    game_index = 4
    game_name = ""
    play_index = 5

    for count, cell_data in enumerate(clean_table, start=1):

        # Add boardgame to collection
        if count == game_index:
            game_name = cell_data
            if boardgames_collection.count_documents({"name": game_name}) <= 0:
                boardgames_collection.insert_one({"name": game_name})

        # Add play data to collection
        if count == play_index:
            ref = boardgames_collection.find_one({"name": game_name})
            play = {"date": date, "numPlays": int(cell_data), "boardgame_id": ref['_id']}
            plays_collection.insert_one(play)

            game_index += 3
            play_index += 3


def get_database():
    client = MongoClient("localhost", 27017)
    db = client['bggPlayDB']
    try:
        client.admin.command('ismaster')
    except (ConnectionError, ServerSelectionTimeoutError):
        logger.error("Connection to Database Failed")
    else:
        return db


def get_clean_data(table_data):
    clean_table_data = []
    for string in table_data:
        new_string = sub(r"([_\-:'!\\.])+", " ", string).title().replace(" ", "")
        clean_table_data.append(new_string[0].lower() + new_string[1:])
    return clean_table_data
