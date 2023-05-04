# Standard Library Imports
import random
import requests
import datetime as dt
import logging
import time

# Third Party Imports
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, InvalidName
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def get_date(num_days):
    return str(dt.date.today() - dt.timedelta(num_days))


def _fetch_bgg_page(date):
    bgg_url = f"https://boardgamegeek.com/geekplay.php?userid=0&startdate={date}&enddate={date}&action=bygame&subtype" \
              f"=All"
    page = requests.get(bgg_url)
    return page.text


def get_bgg_play_data(num_days=1):
    """Parses scraped data and returns a dict of play data"""
    for i in range(num_days, 0, -1):
        date = get_date(i)
        soup = BeautifulSoup(_fetch_bgg_page(date), "html.parser")
        write_bgg_date(soup.findAll("table", {"class": "forum_table"})[1].stripped_strings, date)
        time.sleep(1)


def write_bgg_date(table_data, date):
    db = get_database()
    boardgames_collection = db["boardGameCollection"]
    date_collection = db["dateCollection"]

    # The first game in the table_data list is at index 4
    game_index = 4
    game_name = ""
    play_index = 5

    for count, cell_data in enumerate(table_data, start=1):

        if count == game_index:
            game_name = cell_data

            # Create a new document if there is no existing document
            if boardgames_collection.count_documents({"name": game_name}) <= 0:
                boardgames_collection.insert_one({"name": game_name, "rgbString": get_rgb_string()})

        if count == play_index:
            boardgame_doc = boardgames_collection.find_one({"name": game_name})
            date_collection.insert_one({"date": date, "playData": {"_id": boardgame_doc["_id"], "playCount": int(cell_data)}})

            game_index += 3
            play_index += 3


def generate_rgb_string():
    r = random.randint(0, 240)
    g = random.randint(0, 240)
    b = random.randint(0, 240)
    return f"rgb({r},{g},{b})"


def get_rgb_string():
    rgb_string = generate_rgb_string()
    return rgb_string if check_db_rgb(rgb_string) else get_rgb_string()


def check_db_rgb(rgb_string):
    db = get_database()
    rgb_collection = db["rgbCollection"]

    if rgb_collection.count_documents({"rgbString": rgb_string}) <= 0:
        rgb_collection.insert_one({"rgbString": rgb_string})
        return True
    else:
        return False


def get_database():
    client = MongoClient("localhost", 27017)
    db = client['bggPlayDB']
    try:
        client.admin.command('ismaster')
    except (ConnectionError, ServerSelectionTimeoutError):
        logger.error("Connection to Database Failed")
    else:
        return db
