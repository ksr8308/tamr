cd /home/pnd/customers-skhynix
source setup.sh
/usr/local/bin/python3 pnd/export.py -t schema
/usr/local/bin/python3 pnd/export.py -t values
/usr/local/bin/python3 pnd/export.py -t master
/usr/local/bin/python3 pnd/export.py -t schema_hist
