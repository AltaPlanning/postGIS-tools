"""
Summary of ``assumptions.py``
-----------------------------

TODO

"""

import os
import platform
import configparser
import getpass
import shutil

print("=+=+=+=+" * 8)
print("Loading PostGIS configurations:")

# Get user and system
THIS_USER = getpass.getuser()
THIS_SYSTEM = platform.system()

# Make filepath to config.txt
if THIS_SYSTEM == "Darwin":
    local_config_folder = f"/Users/{THIS_USER}/pGIS"
    local_config_file = os.path.join(local_config_folder, "config.txt")

    # Make it from the -sample.txt if it does not yet exist
    if not os.path.exists(local_config_file):
        print(f"Local config file does not yet exist. Copying to {local_config_folder}")
        os.mkdir(local_config_folder)
        shutil.copyfile("../config-sample.txt", local_config_file)

print(f"\t - {THIS_USER} on {THIS_SYSTEM}, using {local_config_file}")

# Parse the config.txt
config = configparser.ConfigParser()
config.read(local_config_file)

# FULL_CONFIG has everything
CONFIG_FULL = {}
for host in config.sections():
    CONFIG_FULL[host] = {key: config[host][key] for key in config[host]}

# This is a placeholder password for PostgreSQL.
# The assumption is that users will pass their own password into the functions that they use
PG_PASSWORD = CONFIG_FULL["localhost"]["password"]

# Regular CONFIG removes the "default_db" and "super_user" variables
CONFIG = {}
for host in CONFIG_FULL:
    CONFIG[host] = {}
    for key in CONFIG_FULL[host]:
        if key not in ["default_db", "super_user"]:
            CONFIG[host][key] = CONFIG_FULL[host][key]

# Print out options defined in configuration file
print("\t - Available configurations:")
for host in CONFIG:
    print(f"\t\t * {host} - {CONFIG[host]}")

print("=+=+=+=+" * 8, "\n")
