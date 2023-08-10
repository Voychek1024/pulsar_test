#!/bin/bash -i

# delete expired logs
for f in $(ls logs/ | grep log | grep -v "$(date '+%Y%m%d')_$HOSTNAME") ; do
    rm ./logs/"$f"
done

# clear current logs
cat /dev/null > ./logs/consumer.dev.log."$(date '+%Y%m%d')_$HOSTNAME"
cat /dev/null > ./logs/producer.dev.log."$(date '+%Y%m%d')_$HOSTNAME"
