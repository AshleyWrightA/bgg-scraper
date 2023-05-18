import logging


def init_local_logger():
    logging.basicConfig(level=logging.DEBUG, filename="logs/error.log", filemode="w",
                        format='%(name)s - %(levelname)s - %(message)s')


def get_local_logger():
    return logging.getLogger(__name__)
