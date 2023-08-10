import datetime as dt
import time
import requests
import random

import os
from bs4 import BeautifulSoup
from bson import ObjectId
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from logger import get_local_logger


class CrudManager:

    def __init__(self):
        self.logger = get_local_logger()
        self.database = self._get_database()
        self.play_collection = self.database["playCollection"]
        self.board_game_collection = self.database["boardGameCollection"]
        self.rgb_collection = self.database["rgbCollection"]

    def _get_environs(self):
        mongodb_connection_string = ""
        try:
            mongodb_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
        except Exception as e:
            self.logger.error("Failed to get environment variables")
        return mongodb_connection_string

    def _get_database(self):
        uri = self._get_environs()
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client['bggPlayDB']
        try:
            client.admin.command('ismaster')
            self.logger.info("Connection to Database Successful")
        except Exception as e:
            self.logger.error("Connection to Database Failed", e)
        return db

    def get_bgg_play_data(self, num_days=14):
        """Fetches scraped data"""
        for i in range(num_days, 0, -1):
            date = self._get_date(i)
            first_page_soup = BeautifulSoup(self._get_bgg_page(date, 1), "html.parser")
            time.sleep(5)
            second_page_soup = BeautifulSoup(self._get_bgg_page(date, 2), "html.parser")
            time.sleep(5)
            self._process_table_data(first_page_soup.findAll("table", {"class": "forum_table"})[1].stripped_strings,
                                     date)
            self._process_table_data(second_page_soup.findAll("table", {"class": "forum_table"})[1].stripped_strings,
                                     date)

        self.logger.debug(f"Completed scraping. Total play documents: {self._count_play_documents()}. "
                          f"Total board game documents: {self._count_board_game_documents()}")

    def _process_table_data(self, table_data, date):
        # The first game in the table_data list is at index 4
        game_index = 4
        play_index = 5
        board_game_id = ""

        for count, cell_data in enumerate(table_data, start=1):
            if count == game_index:
                game_name = cell_data
                board_game_id = self._find_board_game_id(game_name)
            if count == play_index:
                try:
                    self._process_play_doc(date, cell_data, board_game_id)
                except ValueError:
                    # Skip over a 'False Cell'.
                    # There is an extra div inside some cells on the stats table.
                    self.logger.debug(f"False Cell, {cell_data}")
                    game_index += 4
                    play_index += 4
                else:
                    game_index += 3
                    play_index += 3

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
    def _get_bgg_page(date, page):
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

    def _find_board_game_id(self, game_name):
        board_game_doc = self.board_game_collection.find_one({"name": game_name})
        if board_game_doc is None:
            self._create_new_board_game_doc(game_name)
            board_game_doc = self._get_board_game_doc(game_name)
        return board_game_doc["_id"]

    def _get_board_game_doc(self, game_name):
        return self.board_game_collection.find_one({"name": game_name})

    def _get_play_doc(self, date, board_game_id):
        return self.play_collection.find_one({"date": date, "boardGame_ref": ObjectId(board_game_id)})

    def _process_play_doc(self, date, cell_data, board_game_id):
        play_count = int(cell_data)
        play_doc = self._get_play_doc(date, board_game_id)

        if self._validate_play_doc_by_date(date, board_game_id):
            old_play_count = play_doc["playCount"]
            is_merged = play_doc["merged"]

            if is_merged == "true":
                self._update_merged_play_doc(date, board_game_id, play_count, "false")
            elif is_merged == "false":
                self._update_merged_play_doc(date, board_game_id, play_count, "true")
            elif int(old_play_count) < play_count and is_merged == "null":
                self._update_play_doc(date, board_game_id, play_count)
        else:
            self._create_new_play_doc(date, cell_data, board_game_id)

    def _validate_play_doc_by_date(self, date, board_game_id):
        if self.play_collection.count_documents({"date": date, "boardGame_ref": ObjectId(board_game_id)}) >= 1:
            return True
        else:
            return False

    def _validate_rgb_string_record(self, rgb_string):
        if self.rgb_collection.count_documents({"rgbString": rgb_string}) <= 0:
            self.rgb_collection.insert_one({"rgbString": rgb_string})
            return False
        else:
            return True
