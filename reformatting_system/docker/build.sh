#!/usr/bin/env sh
# rm rorefcat.tar 

if [ -e rorefcat.tar ]; then 
  tar uvf rorefcat.tar `find rorefcat -type f -mnewer rorefcat.tar -print | grep -v "__pycache__"`
else
  tar cvf rorefcat.tar `find rorefcat -type f -print | grep -v "__pycache__"`
fi 

docker build -t ro-processing-framework .

