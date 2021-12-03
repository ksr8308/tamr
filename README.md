# Schema discovery
This is a python package for preparing metadata for and running schema discovey.
Currently supported data sources are Hive2 and Oracle databases.
It has been tested with Python 3.6.5

## Pre-requisites
It is recommended to run the package in a python virtual environment. 
All libraries listed in requirements.txt are necessary.
To set up, run the following commands:
```
virtualenv --python=python3.6 venv
source venv/bin/activate 
pip install --upgrade pip
pip install -r requirements.txt 
```

For `pyhive` and its dependencies, it's expected that `gcc` is already installed.
If not, install it on Ubuntu with `sudo apt-get install gcc` or other correspondent
commands if not Ubuntu.

For `cx_Oracle` to run, you may need to install https://oracle.github.io/odpi/doc/installation.html#oracle-instant-client-rpm. 
Remember to change the version number to what's actually installed. You may need to make a symbolic link 
in the lib directory for `libclntsh.so`.

## conf/
In the conf folder, there should be only two types of files:

  - `creds.yaml`: Contains the necessary credentials for connecting to Tamr Unify, Oracle or Hive2.
  - `schema-discovery-project.yaml`: Configuration files for Unify projects. 

## src/
Source python scripts live under this folder. The main thread is run.py. To run the package, execute:
```
source setup.sh
usage: run.py [-h] {sample,metadata,unify} ...
```

The different subcommands are as follows: 
  1. `run.py sample [-h] -m {local,connect} -s {hive,oracle} -n NAME`: Sample data from `hive` or `oracle` directly 
    to local or to Unify (via df-connect)
    The option `local` will download sampled data in `csv` files into folder `data`. 
    Option `connect` will call `df-connect` husk service to profile data from `oracle` and saved metadata into Unify
    as a dataset. Each option requires necessary information to be provided in `creds.yaml` config file.
  2. `run.py metadata [-h] -m {local,connect} [-s {hive,oracle}] [-n NAME] [-d DICT]`: Process sampled data to generate
    metadata file. Option `local` will process csv files under folder `data` and save metadata file into folder `output`.
    Option `connect` will stream the specified 
    metadata dataset in Unify and save processed metadata file into folder `output`. A dictionary file can be provided 
    for normalizing tokens in column_name.
  3. `run.py unify [-h]`: Run Unify. If the project specified in `schema-discovery-project.yaml` file does not exist yet it will bootstrap a new 
    mastering project. Otherwise it will run through the whole mastering workflow. If `-r`, input datasets will be truncated 
    first before dataset is uploaded again

## data/
Temporary folder for storing source data files retrieved from hive2 server

## output/
Temporary folder for storing metadata file generated using source data files under data/

## logs/
Log files, one per date

## To set up hive2 server for tests
The easiest way to spin up a hive2 server for test purposes is to use docker container. 
  1. Install the latest docker-compose following https://docs.docker.com/compose/install/ 
     ```
        sudo curl -L "https://github.com/docker/compose/releases/download/1.23.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
     ```
  2. Check out the github repo https://github.com/big-data-europe/docker-hive and run
     ```docker-compose up -d```

The hive2 server should then be available at port 10000.
Note that the port number 5432 is used for a postgresql docker container.
Make sure that it's not already taken by the one from Unify. 

See `src/hive2-load-data-example.py` for some example python code for loading data.
