#!/usr/bin/zsh

# spark-shell -i ./DataPrepare.scala 2> /dev/null 1> log_DataPrepare.txt

filelist=(
# "DataPrepare"

# "ClearCache" 
# "DropView"
# "EX1Q1" 
# "EX1Q2" 
# "EX1U1" 
# "EX1V1" 
# "EX1V2"

# "ClearCache" 
# "DropView"
# "EX2Q1" 
# "EX2Q2" 
# "EX2U1" 
# "EX2V1" 
# "EX2V2"

"ClearCache" 
"DropView"
"EX2Q"
# "EX2V"
"EX2V_bk"
)

for file in ${filelist[@]}
do
echo $file
spark-shell --master "spark://Jiahao:7076" -i ./$file.scala 2> log_out_$file.txt 1> log_$file.txt
done

echo "Done"