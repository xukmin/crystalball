#!/bin/bash
./build.sh || exit

rm -f benchmark.txt
for ((i = 150; i >= 1; i--)); do
  echo -n "${i} " >> benchmark.txt
  hadoop jar bin/crystal.jar org.xukmin.crystal.PostMapReduce \
      -D mapreduce.job.reduces=${i} >> benchmark.txt
done
