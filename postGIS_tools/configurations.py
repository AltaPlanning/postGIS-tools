"""
Summary of ``configurations.py``
-----------------------------

TODO

Examples
--------

    >>> from postGIS_tools.configurations import get_postGIS_config
    >>> config, _ = get_postGIS_config()
    - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ -
    LOADING postGIS CONFIGURATIONS FROM /Users/aaron/Documents/pGIS-configurations/config.txt
        * localhost *
             {'host': 'localhost', 'username': 'postgres', 'password': 'your-password-here', 'port': '5432'}

        * digitalocean *
             {'username': 'your-username-here', 'host': 'your-host-here.db.ondigitalocean.com', 'password': 'your-password-here', 'port': '98765'}

    - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ - - @ -
"""

import os
import platform
import configparser
import getpass
import shutil
from typing import Union


PG_PASSWORD = "this-is-a-placeholder-password"

THIS_SYSTEM = platform.system()

SEPARATOR = "- @ - " * 14

LOCAL_USER_CONFIG_FOLDER = "pGIS-configurations"


def get_postGIS_config(
        custom_config_file: Union[bool, str] = None,
        verbose: bool = True
) -> tuple:
    """
    Read a config.txt file and return a tuple wiht dictionaries for each postGIS configuration defined,
    Simple CONFIG is keyed on host, username, password, and port.
    CONFIG_FULL also has keys for super_user and default_db

    :param custom_config_path:
    :param verbose: boolean to print out configuration values
    :return:
    """
    print(SEPARATOR)

    # Use the specified custom file if provided
    if custom_config_file:
        config_file = custom_config_file

    # Otherwise, figure out the filepath dynamically by finding
    # the user's OS-specific "Documents" folder.
    else:
        # Get user and system
        THIS_USER = getpass.getuser()

        # Make filepath to config.txt
        if THIS_SYSTEM == "Darwin":
            local_config_folder = f"/Users/{THIS_USER}/Documents/{LOCAL_USER_CONFIG_FOLDER}"

        elif THIS_SYSTEM == "Windows":
            local_config_folder = rf"C:\Users\{THIS_USER}\My Documents\{LOCAL_USER_CONFIG_FOLDER}"

        # Build the path to the "config.txt" file
        config_file = os.path.join(local_config_folder, "config.txt")

        # Make it by copying the config-sample.txt if it does not yet exist
        if not os.path.exists(config_file):

            # Make the folder if it does not yet exist:
            if not os.path.exists(local_config_folder):
                os.mkdir(local_config_folder)

            # Copy the sample file
            shutil.copyfile("../config-sample.txt", config_file)

    print(f"LOADING postGIS CONFIGURATIONS FROM {config_file}")

    # Parse the config.txt
    config = configparser.ConfigParser()
    config.read(config_file)

    # Make a SUPERUSER_CONFIG dict with super user info and CONFIG without the keys_to_skip
    CONFIG = {}
    SUPERUSER_CONFIG = {}
    keys_to_skip = ["default_db", "super_user", "super_user_pw"]
    for host in config.sections():
        CONFIG[host] = {key: config[host][key] for key in config[host] if key not in keys_to_skip}

        SUPERUSER_CONFIG[host] = {key: config[host][key] for key in config[host]}

        SUPERUSER_CONFIG[host]["password"] = SUPERUSER_CONFIG[host].pop("super_user_pw")
        SUPERUSER_CONFIG[host]["username"] = SUPERUSER_CONFIG[host].pop("super_user")

    # Print out options defined in configuration file
    if verbose:
        for host in CONFIG:
            print(f"\t* {host} *")
            print(f"\t\t {CONFIG[host]} \n")

    print(SEPARATOR, "\n")

    return CONFIG, SUPERUSER_CONFIG

if __name__ == "__main__":
    get_postGIS_config()