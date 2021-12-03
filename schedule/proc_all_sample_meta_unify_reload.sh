cd /home/pnd/customers-skhynix
source setup.sh
/usr/local/bin/python3 src/run.py sample -m connect -s oracle -n mdm -r
/usr/local/bin/python3 src/run.py sample -m connect -s oracle -n mixed -r
/usr/local/bin/python3 src/run.py sample -m connect -s oracle -n legacy -r
/usr/local/bin/python3 src/run.py metadata -m connect -s oracle -n mdm -d /home/pnd/customers-skhynix/token_tamr_combined.csv
/usr/local/bin/python3 src/run.py metadata -m connect -s oracle -n mixed -d /home/pnd/customers-skhynix/token_tamr_combined.csv
/usr/local/bin/python3 src/run.py metadata -m connect -s oracle -n legacy -d /home/pnd/customers-skhynix/token_tamr_combined.csv
/usr/local/bin/python3 src/run.py unify -r