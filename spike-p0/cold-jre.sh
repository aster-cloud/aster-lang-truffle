#!/bin/bash
# 10 次独立进程冷启动墙钟（ms），打印每次与中位数；stdout 只应为结果
j=$1; shift
ts=()
for i in $(seq 1 10); do
  s=$(date +%s%N)
  out=$(java -jar /w/$j /w/test_life.canon.json 1 40 20 2>/dev/null)
  e=$(date +%s%N)
  ts+=($(( (e-s)/1000000 )))
done
echo "stdout=[$out]"
echo "runs=${ts[*]}"
printf '%s\n' "${ts[@]}" | sort -n | awk '{a[NR]=$1} END {print "median=" (a[5]+a[6])/2 "ms min=" a[1] " max=" a[NR]}'
