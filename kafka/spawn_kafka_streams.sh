#!/bin/bash
NUM_SPAWNS=$1
END=`echo "$NUM_SPAWNS - 1" | bc`
SESSION=$2
tmux new-session -s $SESSION -n bash -d
for ID in `seq 0 $END`;
do
    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'python3 kafka_producer.py '"$NUM_SPAWNS"' '"$ID"'' C-m
done
