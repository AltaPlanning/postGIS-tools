import os
import platform
import configparser


config = configparser.ConfigParser()

if os.path.exists("../config.txt"):
    config_path = "../config.txt"
else:
    config_path = "../config-sample.txt"

config.read(config_path)

configurations = {}
for host in ["DEFAULT", "work_cpu", "digitalocean"]:
    configurations[host] = {key: config[host][key] for key in config[host]}

for section in configurations:
    print(configurations[section])

# This is a placeholder password for PostgreSQL.
# The assumption is that users will pass their own password into the functions that they use
PG_PASSWORD = configurations["DEFAULT"]["password"]

THIS_SYSTEM = platform.system()
