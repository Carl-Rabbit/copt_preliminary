#!/usr/bin/zsh

spark-shell -i \
  ./MainCache.scala \
  2> ./log_cache.txt 1> output_cache.txt
