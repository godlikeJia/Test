#!/bin/sh
days=30

for i in $(seq $days)
do
 	day="date -d \"$i day\" +%Y%m%d"
	day=`eval $day`
	echo $day
done
