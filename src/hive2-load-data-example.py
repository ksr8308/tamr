from pyhive import hive

conn = hive.Connection(
    host="10.20.0.193",
    port=10000,
    username="hive",
    password="hive",
    database="testing",
    auth="CUSTOM"
)

cur = conn.cursor()

# create non-partitioned table
cur.execute("""CREATE TABLE supplier_table (
                Address STRING,
                Name STRING,
                Zipcode STRING,
                State STRING,
                Country STRING,
                DUNS STRING,
                City STRING,
                Tamr_Source STRING,
                Contact_Email STRING,
                Contact_Fax STRING,
                Contact_Name STRING,
                Contact_Phone STRING,
                Spend DOUBLE,
                tamrUniqueId STRING,
                modifiedDate STRING,
                modifiedTime STRING,
                modifiedTimestamp STRING
                )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\\"",
   "escapeChar"    = "\\\\"
)
""")

# load data from local csv file (here local path is the path from within the docker container)
cur.execute("load data local inpath '/opt/hive/supplier.csv' into table supplier_table")

# create partitioned table
cur.execute("""CREATE TABLE supplier_partitioned (
                Address STRING,
                Name STRING,
                Zipcode STRING,
                DUNS STRING,
                City STRING,
                Tamr_Source STRING,
                Contact_Email STRING,
                Contact_Fax STRING,
                Contact_Name STRING,
                Contact_Phone STRING,
                Spend DOUBLE,
                tamrUniqueId STRING,
                modifiedDate STRING,
                modifiedTime STRING,
                modifiedTimestamp STRING
                )
        PARTITIONED BY (Country STRING, State STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
           "separatorChar" = ",",
           "quoteChar"     = "\\"",
           "escapeChar"    = "\\\\"
        )
""")
cur.execute("""set hive.exec.max.dynamic.partitions=500""")
cur.execute("""set hive.exec.max.dynamic.partitions.pernode=500""")

# copy data from another table
cur.execute("""
FROM supplier_table t
INSERT OVERWRITE TABLE supplier_partitioned PARTITION(Country='US', State)
       SELECT t.Address, t.Name, t.Zipcode, t.DUNS, t.City,
              t.Tamr_Source, t.Contact_Email, t.Contact_Fax, t.Contact_Name,
              t.Contact_Phone, t.Spend, t.tamrUniqueId, t.modifiedDate,
              t.modifiedTime, t.modifiedTimestamp, t.State
""")

