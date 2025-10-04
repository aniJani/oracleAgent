# config.py

import configparser
import os
import sys
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
from platformdirs import user_config_dir, user_data_dir

# --- Define AppName and Author ---
# These are used by platformdirs to create a standard path.
APP_NAME = "AxessProtocol"
APP_AUTHOR = "AxessProtocol"

# --- 1. Determine the user-specific config directory ---
# This will be C:\Users\YourUser\AppData\Roaming\AxessProtocol\AxessProtocol on Windows,
# or ~/.config/AxessProtocol on Linux/macOS.
CONFIG_DIR = Path(user_config_dir(APP_NAME, APP_AUTHOR))
CONFIG_DIR.mkdir(parents=True, exist_ok=True) # Ensure the directory exists
USER_CONFIG_PATH = CONFIG_DIR / "config.ini"

DATA_DIR = Path(user_data_dir(APP_NAME, APP_AUTHOR))
SESSIONS_DIR = DATA_DIR / "sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


def load_all_settings() -> Dict:
    """
    Loads configuration from both the bundled .env and the user's config.ini,
    returning a single merged dictionary.
    """
    settings = {}
    
    # --- 2. Load from Environment Variables (from bundled .env file) ---
    # PyInstaller bundles the .env file, so we need to find it.
    if getattr(sys, 'frozen', False):
        # In a bundled app, the .env file is in the temp folder with the scripts
        base_path = Path(sys._MEIPASS)
    else:
        # In development, it's next to this file
        base_path = Path(__file__).resolve().parent
    
    env_path = base_path / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    
    required_env_vars = {
        "aptos_contract_address": "APTOS_CONTRACT_ADDRESS",
        "aptos_node_url": "APTOS_NODE_URL",
        "backend_ws_url": "BACKEND_WS_URL",
    }
    for key, env_var in required_env_vars.items():
        value = os.getenv(env_var)
        if not value:
            raise ValueError(f"Required developer setting '{env_var}' not found in .env file.")
        settings[key] = value

    # --- 3. Load from user's config.ini in the standard AppData location ---
    try:
        if not USER_CONFIG_PATH.exists():
             raise FileNotFoundError(
                f"config.ini not found in {CONFIG_DIR}.\n"
                "Please run the app and use the Settings dialog to create it."
             )

        user_config = configparser.ConfigParser()
        user_config.read(USER_CONFIG_PATH)

        settings["private_key"] = user_config.get("aptos", "private_key")
        settings["price_per_second"] = user_config.getint("host", "price_per_second")
        
        if not settings["private_key"]:
            raise ValueError("private_key is missing from your config.ini.")

    except (configparser.Error, KeyError, FileNotFoundError) as e:
        raise ValueError(f"Error reading user settings from config.ini: {e}") from e

    return settings

def save_user_settings(private_key: str, price_per_second: int):
    """
    Saves the user-specific settings to config.ini in the AppData directory.
    """
    user_config = configparser.ConfigParser()
    user_config.add_section("aptos")
    user_config.set("aptos", "private_key", private_key)
    user_config.add_section("host")
    user_config.set("host", "price_per_second", str(price_per_second))
    
    with open(USER_CONFIG_PATH, "w", encoding="utf-8") as f:
        user_config.write(f)

def load_user_settings() -> configparser.ConfigParser:
    """
    Loads the user-specific settings from config.ini for the GUI.
    Returns an empty parser if the file doesn't exist.
    """
    user_config = configparser.ConfigParser()
    if USER_CONFIG_PATH.exists():
        user_config.read(USER_CONFIG_PATH)
    return user_config