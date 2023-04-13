from scrape import get_bgg_play_data
from logger import initLogger


if __name__ == "__main__":
    initLogger()
    get_bgg_play_data(1)
