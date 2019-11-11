"""
Summary of ``configurations.py``
-----------------------------

TODO

"""

import os
import platform
import configparser
import getpass
import shutil
from typing import Union


PG_PASSWORD = "this-is-a-placeholder-password"


def get_postGIS_config(
        custom_config_folder: Union[bool, str] = None
) -> tuple:
    """
    Read a config.txt file and return a tuple wiht dictionaries for each postGIS configuration defined,
    Simple CONFIG is keyed on host, username, password, and port.
    CONFIG_FULL also has keys for super_user and default_db

    :param custom_config_path:
    :return:
    """
    print(r"//\\//\\" * 10)
    print("Loading PostGIS configurations:")

    # Use the specified custom folder if provided
    if custom_config_folder:
        local_config_folder = custom_config_folder

    # Otherwise, figure out the location dynamically by finding
    # the user's OS-specific "Documents" folder
    else:
        # Get user and system
        THIS_USER = getpass.getuser()
        THIS_SYSTEM = platform.system()

        # Make filepath to config.txt
        if THIS_SYSTEM == "Darwin":
            local_config_folder = f"/Users/{THIS_USER}/Documents/pGIS"

    # Build the path to the "config.txt" file
    local_config_file = os.path.join(local_config_folder, "config.txt")

    # Make it by copying the config-sample.txt if it does not yet exist
    if not os.path.exists(local_config_file):
        print(f"\t - Local config file does not yet exist. Copying to {local_config_folder}")
        os.mkdir(local_config_folder)
        shutil.copyfile("../config-sample.txt", local_config_file)

    print(f"\t - {THIS_USER} on {THIS_SYSTEM}, using {local_config_file}")

    # Parse the config.txt
    config = configparser.ConfigParser()
    config.read(local_config_file)

    # Make a CONFIG_FULL dict with everything and CONFIG without the keys_to_skip
    CONFIG_FULL = {}
    keys_to_skip = ["default_db", "super_user"]
    CONFIG = {}

    for host in config.sections():
        CONFIG_FULL[host] = {key: config[host][key] for key in config[host]}
        CONFIG[host] = {key: config[host][key] for key in config[host] if key not in keys_to_skip}

    # This is a placeholder password for PostgreSQL.
    # The assumption is that users will pass their own password into the functions that they use
    PG_PASSWORD = CONFIG_FULL["localhost"]["password"]

    # # Regular CONFIG removes the "default_db" and "super_user" variables
    # CONFIG = {}
    # for host in CONFIG_FULL:
    #     CONFIG[host] = {}
    #     for key in CONFIG_FULL[host]:
    #         if key not in ["default_db", "super_user"]:
    #             CONFIG[host][key] = CONFIG_FULL[host][key]

    # Print out options defined in configuration file
    print("\t - Available configurations:")
    for host in CONFIG:
        print(f"\t\t * {host} - {CONFIG[host]}")
        for key in keys_to_skip:
            print(f"\t\t\t + {key} : {CONFIG_FULL[host][key]}")
    print(r"\\//\\//" * 10, "\n")

    return (CONFIG, CONFIG_FULL)

if __name__ == "__main__":
    get_postGIS_config()