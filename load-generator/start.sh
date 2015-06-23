#!/usr/bin/env bash
# List of hosts, time interval, duration
# Given a time interval (t) and duration (d), loads a host during d seconds
# every t seconds

targets=()
RANDOM=$$$(date +%s)

# String (File) -> Array[String]
# Reads all lines in a file and returns an array with 1 line = 1 array element
getArray() {
    i=0
    while read line # Read a line
    do
        targets[i]=$line # Put it into the array
        i=$(($i + 1))
    done < $1
}

# Array[String] -> String
# Given an array containing target hosts, returns a random host = element of the array
getTarget() {
  target=${targets[$(($RANDOM % ${#targets[*]}))]}
}

timestamp(){
   while read line
      do
         echo -e "\e[34m**`date`**\e[0m $line"
      done
}

# Initializing targets from file
getArray "target"

echo "A host will be put under load during $2 second(s) every $1 second(s)"
while [ 1 ]
do
  getTarget
  # echo "$target during $2 seconds"
  echo "Loading [[$target]]" | timestamp
  ssh enx@$target "stress -t $2 -c 1 -d 1" |timestamp
  echo "Putting next host under stress in $1 seconds..." | timestamp
  sleep $1
done

# getArray "target"
# getTarget
# # t = ${targets[$RANDOM % ${#RANDOM[*]}]}
# # echo $t
# for e in "${targets[@]}"
# do
#     echo "$e"
#     # t=$(getTarget)
#     getTarget
#     echo $target
#
#     # echo ${targets[$(($RANDOM % ${#targets[*]}))]}
# done
