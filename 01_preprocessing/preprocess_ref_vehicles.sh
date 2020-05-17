#!/bin/bash

export rawDir=../00_rawdata/
export rawFilename=vehicles.csv.zip
export unzippedFilename="vehicles.csv"

export ouputDir=./ref_vehicle/
export outputFilename=ref_vehicle.txt

echo "rawDir           : ${rawDir}"
echo "rawFilename      : ${rawFilename}"
echo "unzippedFilename : ${unzippedFilename}"
echo "ouputDir         : ${ouputDir}"
echo "outputFilename   : ${outputFilename}"

echo "Resetting ouputDir Directory : ${ouputDir}"
rm -rf ${ouputDir}
mkdir ${ouputDir}

echo "Copying ${rawDir}${rawFilename} to ${ouputDir}"
cp ${rawDir}${rawFilename} ${ouputDir}

echo "Unzipping ${ouputDir}${rawFilename}"
unzip ${ouputDir}${rawFilename} -d ${ouputDir}

echo "Preparing file format ${ouputDir}${unzippedFilename}"
python3 preprocess_ref_vehicle.py ${ouputDir}${unzippedFilename} > ${ouputDir}${outputFilename}

rm ${ouputDir}${rawFilename}
rm ${ouputDir}${unzippedFilename}
ls -al ${ouputDir}
head ${ouputDir}${outputFilename}