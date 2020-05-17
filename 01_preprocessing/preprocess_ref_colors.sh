#!/bin/bash

export rawDir=../00_rawdata/
export rawFilename=colornames.zip
export unzippedFilename=colornames.txt

export ouputDir=./ref_colors/
export outputFilename=ref_colors.txt

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
python3 preprocess_ref_colors.py ${ouputDir}${unzippedFilename} > ${ouputDir}${outputFilename}

rm ${ouputDir}${rawFilename}
rm ${ouputDir}${unzippedFilename}
ls -al ${ouputDir}
head ${ouputDir}${outputFilename}