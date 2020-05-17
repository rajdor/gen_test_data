#!/bin/bash

echo "########################################################################"
echo "# REF_CODES"
echo "########################################################################"
export rawDir=../00_rawdata/
export rawFilename=ref_codes.xlsx
export outputDir=./ref_codes/

echo "rawDir        : ${rawDir}"
echo "rawFilename   : ${rawFilename}"
echo "outputDir     : ${outputDir}"

echo "Resetting outputDir Directory : ${outputDir}"
rm -rf ${outputDir}
mkdir ${outputDir}

echo "Copying ${rawDir}${rawFilename} to ${outputDir}"
cp ${rawDir}${rawFilename} ${outputDir}

echo "Preparing files format ${outputDir}${rawFilename}"
python3 preprocess_ref_codes.py ${outputDir}${rawFilename} ${outputDir}

rm ${outputDir}${rawFilename}
ls -al ${outputDir}