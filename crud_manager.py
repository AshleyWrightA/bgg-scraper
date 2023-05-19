import datetime as dt
import time
import requests
import random

from bs4 import BeautifulSoup
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from logger import get_local_logger


class CrudManager:

    def __init__(self):
        self.database = self._get_database()
        self.date_collection = self.database["dateCollection"]
        self.play_collection = self.database["playCollection"]
        self.board_game_collection = self.database["boardGameCollection"]
        self.rgb_collection = self.database["rgbCollection"]
        self.logger = get_local_logger()

    def _get_database(self):
        client = MongoClient("localhost", 27017)
        db = client['bggPlayDB']
        try:
            client.admin.command('ismaster')
        except (ConnectionError, ServerSelectionTimeoutError):
            self.logger.error("Connection to Database Failed")
        else:
            return db

    def _validate_date_record(self, date):
        """Checks the dateCollection collection which contains a record of each date's data that was scraped into the
        database. Returns false if date lacks any scraped data."""

        # Check if data was already entered for this date, if false, fetch and write data
        if self.date_collection.count_documents({"date": date}) <= 0:
            self.date_collection.insert_one({"date": date})
            return False
        else:
            return True

    def get_bgg_play_data(self, num_days=14):
        """Fetches scraped data"""
        for i in range(num_days, 0, -1):
            date = self._get_date(i)
            first_page_soup = BeautifulSoup(self._fetch_bgg_page(date, 1), "html.parser")
            time.sleep(5)
            second_page_soup = BeautifulSoup(self._fetch_bgg_page(date, 2), "html.parser")
            time.sleep(5)
            self._process_play_data(first_page_soup, second_page_soup, date)
        self.logger.debug(f"Completed scraping. Total play documents: {self._count_play_documents()}. "
                          f"Total board game documents: {self._count_board_game_documents()}")

    def create_bgg_data(self, table_data, date):
        """Creates new play & boardgame data entries if a new game is scraped."""

        # The first game in the table_data list is at index 4, skipping the headers
        game_index = 4
        play_index = 5
        game_name = ""

        for count, cell_data in enumerate(table_data, start=1):
            if count == game_index:
                game_name = cell_data
                # Create a new document in the boardGameCollection if there is no existing document
                if self.board_game_collection.count_documents({"name": game_name}) == 0:
                    self._create_new_board_game_doc(game_name)

            if count == play_index:
                play_count = int(cell_data)
                board_game_id = self._get_board_game_id(game_name)
                # Check if a play has already been entered, with a matching date
                if self.play_collection.count_documents({"date": date, "boardGame_ref": ObjectId(board_game_id)}) >= 1:
                    self._update_merged_play_doc(date, board_game_id, play_count, "true")
                else:
                    self._create_new_play_doc(date, play_count, board_game_id)

                game_index += 3
                play_index += 3

    def update_bgg_date(self, table_data, date):
        """Updates existing play data entries. Overwrites the entry if an updated count is scraped."""

        # The first game in the table_data list is at index 4
        game_index = 4
        play_index = 5
        game_name = ""
        board_game_id = ""

        for count, cell_data in enumerate(table_data, start=1):

            if count == game_index:
                game_name = cell_data
                board_game_id = self._get_board_game_id(game_name)

            if count == play_index:

                # If a record appears on the table during an update, due to retro logged plays, add it to the DB
                if board_game_id is None:
                    self._create_new_board_game_doc(game_name)
                    board_game_id = self._get_board_game_id(game_name)
                    self._create_new_play_doc(date, game_name, board_game_id)
                else:
                    play_count = int(cell_data)
                    play_doc = self._get_play_doc(date, board_game_id)
                    old_play_count = play_doc["playCount"]
                    is_merged = play_doc["merged"]

                    if is_merged == "true":
                        self._update_merged_play_doc(date, board_game_id, play_count, "false")
                    if is_merged == "false":
                        self._update_merged_play_doc(date, board_game_id, play_count, "true")
                    if old_play_count < play_count and is_merged == "null":
                        self._update_play_doc(date, board_game_id, play_count)

                game_index += 3
                play_index += 3

    def _process_play_data(self, first_page, second_page, date):
        if not self._validate_date_record(date):
            self.create_bgg_data(first_page.findAll("table", {"class": "forum_table"})[1].stripped_strings, date)
            self.create_bgg_data(second_page.findAll("table", {"class": "forum_table"})[1].stripped_strings, date)
        else:
            self.update_bgg_date(first_page.findAll("table", {"class": "forum_table"})[1].stripped_strings, date)
            self.update_bgg_date(second_page.findAll("table", {"class": "forum_table"})[1].stripped_strings, date)

    def _validate_rgb_string_record(self, rgb_string):
        if self.rgb_collection.count_documents({"rgbString": rgb_string}) <= 0:
            self.rgb_collection.insert_one({"rgbString": rgb_string})
            return False
        else:
            return True

    def _get_rgb_string(self):
        rgb_string = self._generate_rgb_string()
        return rgb_string if not self._validate_rgb_string_record(rgb_string) else self._get_rgb_string()

    @staticmethod
    def _generate_rgb_string():
        r = random.randint(10, 240)
        g = random.randint(10, 240)
        b = random.randint(10, 240)
        return f"rgb({r},{g},{b})"

    @staticmethod
    def _get_date(num_days):
        return str(dt.date.today() - dt.timedelta(num_days))

    @staticmethod
    def _fetch_bgg_page(date, page):
        bgg_url = f"https://boardgamegeek.com/plays/bygame/subtype/All/start/{date}/end/{date}/page/{page}"
        page = requests.get(bgg_url)
        return page.text

    def _count_play_documents(self):
        return self.play_collection.count_documents({})

    def _count_board_game_documents(self):
        return self.board_game_collection.count_documents({})

    def _create_new_board_game_doc(self, game_name):
        self.board_game_collection.insert_one({"name": game_name, "rgbString": self._get_rgb_string()})
        self.logger.debug(f"inserted new boardGameCollection document: {game_name}")

    def _create_new_play_doc(self, date, cell_data, board_game_id):
        self.play_collection.insert_one({"date": date, "playCount": cell_data, "boardGame_ref": board_game_id,
                                         "merged": "null"})

    def _update_merged_play_doc(self, date, board_game_id, play_count, merged_bool):
        self.play_collection.update_one({"date": date, "boardGame_ref": ObjectId(board_game_id)},
                                        {"$inc": {"playCount": play_count}, "$set": {"merged": merged_bool}})

    def _update_play_doc(self, date, board_game_id, play_count):
        self.play_collection.update_one({"date": date, "boardGame_ref": ObjectId(board_game_id)},
                                        {"$set": {"playCount": play_count}})
        self.logger.debug(f"play record updated: {board_game_id}")

    def _get_board_game_id(self, game_name):
        board_game_id = self.board_game_collection.find_one({"name": game_name})
        if board_game_id is None:
            return None
        else:
            return board_game_id["_id"]

    def _get_play_doc(self, date, board_game_id):
        return self.play_collection.find_one({"date": date, "boardGame_ref": ObjectId(board_game_id)})
