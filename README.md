
Building the executable jar: 
mvm -P fatjar clean install

java -jar target/hadoop-hbase-test-0.0.1-SNAPSHOT-jar-with-dependencies.jar <server> <table-name>