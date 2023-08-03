from logger import init_logger
from crud_manager import CrudManager


def main():
    init_logger()
    manager = CrudManager()
    manager.get_bgg_play_data()


# Entrypoint
if __name__ == "__main__":
    main()
