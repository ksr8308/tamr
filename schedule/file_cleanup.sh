cd /home/pnd/customers-skhynix
source setup.sh
sudo chmod -R 777 /backup/unify_backup
/usr/local/bin/python3 pnd/file_cleanup.py
sudo chmod -R 755 /backup/unify_backup
