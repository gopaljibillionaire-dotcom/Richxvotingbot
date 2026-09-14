import os
import sys
import logging

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MultiAccountSystem")

# --- CREDENTIALS ---
API_ID = int(os.getenv("TG_API_ID", "30861364"))
API_HASH = os.getenv("TG_API_HASH", "2597cb5e23d6eaa3712fd58d814927d7")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8898776916:AAE3LpiImsXGIRM3vW6nqrgRi2Fk82FobXg")

# --- DATABASE ---
MONGO_URI =  "mongodb+srv://gopaljichoubey:gopaljichoubey12@cluster0.imgbed7.mongodb.net/?appName=Cluster0"

# HARDCODED SUPER-OWNER IDS
SUPER_OWNER_IDS = [8188380498, 7952327997, 7953147643, 8064493735] 

# DEVELOPER ATTRIBUTIONS
DESIGNER_HANDLE = "Gopalji_choubey"
MANAGER_HANDLE = "BMWM4Z"

# CRYPTO KEY FOR LOCAL DATABASE OBFUSCATION
SECRET_KEY = os.getenv("ENCRYPTION_KEY", "secure_fallback_key_2026")

# AUDIT CHANNEL FOR TELEGRAM LOG EVENTS
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1003917762371"))
