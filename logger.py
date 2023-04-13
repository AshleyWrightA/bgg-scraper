import logging

def initLogger():
    logging.basicConfig(level=logging.DEBUG, filename="logs/error.log", filemode="w",
                        format='%(name)s - %(levelname)s - %(message)s')
