DAY=`date +%d`
if [ $DAY -le 7 ] ; then
	cd /home/pnd/customers-skhynix
	source setup.sh
	curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' --header 'Authorization: BasicCreds YWRtaW46ZHQ=' 'http://10.38.11.54:9100/api/backups'
fi
