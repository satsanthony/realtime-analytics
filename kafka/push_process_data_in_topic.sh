export PATH=$PATH:/home/ec2-user/kafka/bin
FILES=$1/*.csv
for f in $FILES
do
    echo "ingesting $f file"
    cat $f | kafka-producer.sh --broker-list $2  --topic $3
    sleep 60
done
