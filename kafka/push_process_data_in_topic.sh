export PATH=$PATH:/home/hadoop/kafka/bin
FILES=$1/*.csv
for f in $FILES
do
    echo "ingesting $f file"
    cat $f | kafka-console-producer.sh --broker-list $2  --topic $3
    sleep 60
done
