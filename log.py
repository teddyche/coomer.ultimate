# log.py
import logging

# Création du logger
logger = logging.getLogger("coomer_logger")
logger.setLevel(logging.DEBUG)

# Formatter commun
formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")

# Handler fichier
file_handler = logging.FileHandler("app.log", mode="a")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

# Handler console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# Évite les doublons
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def log_info(msg):
    logger.info(msg)

def log_error(msg):
    logger.error(msg)

def log_debug(msg):
    logger.debug(msg)

def log_warning(msg):
    logger.warning(msg)
