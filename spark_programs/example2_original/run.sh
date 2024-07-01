#!/usr/bin/zsh

spark-shell -i \
  ./Main.scala \
  2> ./log.txt 1> output.txt
