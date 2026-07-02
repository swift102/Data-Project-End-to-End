# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3eb590da-d3b9-45f8-8f0e-1171e9ac479e",
# META       "default_lakehouse_name": "lh_bronze_banking_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "3eb590da-d3b9-45f8-8f0e-1171e9ac479e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json

# Repository coordinates 
_GITHUB_HOST  = "https://github.com"
_GITHUB_USER  = "inhamo"
_GITHUB_REPO  = "Datasets-Advanced-2026"
MASK_SALT = "keystone_2026"

config = {
    "github_url":    f"{_GITHUB_HOST}/{_GITHUB_USER}/{_GITHUB_REPO}.git",
    "raw_path":      "Files/raw",
    "bronze_schema": "bronze",
    "MASK_SALT":     MASK_SALT,   
}

mssparkutils.notebook.exit(json.dumps(config))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data masking salt value


# CELL ********************

MASK_SALT = "keystone_2026"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
