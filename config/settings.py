import os
import json

def load_api_keys():
    db_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "accounts_db.json"))
    if os.path.exists(db_path):
        with open(db_path, "r") as f:
            db = json.load(f)
        return [(acc["apiKey"], acc.get("walletAddress", "")) for acc in db.get("accounts", [])]
    return [(os.environ.get("API_KEY", ""), os.environ.get("WALLET_ADDRESS", ""))]

ALL_ACCOUNTS = load_api_keys()

API_KEY = ALL_ACCOUNTS[0][0] if ALL_ACCOUNTS else os.environ.get("API_KEY", "")
BASE_URL = os.environ.get("BASE_URL", "https://cdn.moltyroyale.com/api")
WALLET_ADDRESS = ALL_ACCOUNTS[0][1] if ALL_ACCOUNTS else os.environ.get("WALLET_ADDRESS", "")
PREFERRED_GAME_TYPE = "free"
AUTO_CREATE_GAME = True
GAME_MAP_SIZE = "medium"
HP_CRITICAL = 65
HP_LOW = 45
EP_MIN_ATTACK = 2
EP_REST_THRESHOLD = 3
WIN_PROBABILITY_ATTACK = 0.65
WIN_PROBABILITY_AGGRESSIVE = 0.80
LEARNING_ENABLED = True
DATA_DIR = "data"
MIN_GAMES_FOR_ML = 5
LEARNING_RATE = 0.1
REDIS_ENABLED = False
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
LOG_LEVEL = "DEBUG"
LOG_TO_FILE = True
LOG_FILE = "logs/bot.log"
TURN_INTERVAL = 60
POLL_INTERVAL_WAITING = 5
POLL_INTERVAL_DEAD = 60
ROOM_HUNT_INTERVAL = 5
HEARTBEAT_INTERVAL = 300
