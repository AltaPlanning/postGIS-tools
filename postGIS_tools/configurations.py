"""
Summary of ``configurations.py``
--------------------------------

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

from postGIS_tools.constants import SEPARATOR

# Get user and system
THIS_USER = getpass.getuser()
THIS_SYSTEM = platform.system()


LOCAL_USER_CONFIG_FOLDER = "pGIS-configurations"

# Make filepaths to User's desktop and documents folders
if THIS_SYSTEM == "Darwin":
    USER_HOME = f"/Users/{THIS_USER}"
    USER_DOCUMENTS_FOLDER = os.path.join(USER_HOME, "Documents")
    USER_DESKTOP = os.path.join(USER_HOME, "Desktop")

elif THIS_SYSTEM == "Windows":
    USER_HOME = rf"C:\Users\{THIS_USER}"
    USER_DOCUMENTS_FOLDER = os.path.join(USER_HOME, "My Documents")
    USER_DESKTOP = os.path.join(USER_HOME, "Desktop")

LOCAL_CONFIG_FOLDER = os.path.join(USER_DOCUMENTS_FOLDER, LOCAL_USER_CONFIG_FOLDER)


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
        # Build the path to the "config.txt" file
        config_file = os.path.join(LOCAL_CONFIG_FOLDER, "config.txt")

        # Make it by copying the config-sample.txt if it does not yet exist
        if not os.path.exists(config_file):

            # Make the folder if it does not yet exist:
            if not os.path.exists(LOCAL_CONFIG_FOLDER):
                os.mkdir(LOCAL_CONFIG_FOLDER)

            # Copy the sample file
            shutil.copyfile("../config-sample.txt", config_file)

    print(f"LOADING postGIS CONFIGURATIONS FROM {config_file}")

    # Parse the config.txt
    config_object = configparser.ConfigParser()
    config_object.read(config_file)

    # Make a SUPERUSER_CONFIG dict with super user info and CONFIG without the keys_to_skip
    config = {}
    superuser_config = {}
    keys_to_skip = ["default_db", "super_user", "super_user_pw"]
    for host in config_object.sections():
        print(host)
        config[host] = {key: config_object[host][key] for key in config_object[host] if key not in keys_to_skip}

        superuser_config[host] = {key: config_object[host][key] for key in config_object[host]}

        superuser_config[host]["password"] = superuser_config[host].pop("super_user_pw")
        superuser_config[host]["username"] = superuser_config[host].pop("super_user")

    # Print out options defined in configuration file
    if verbose:
        for host in config:
            print(f"\t* {host} *")
            print(f"\t\t {config[host]} \n")

    print(SEPARATOR, "\n")

    return config, superuser_config


if __name__ == "__main__":
    get_postGIS_config()