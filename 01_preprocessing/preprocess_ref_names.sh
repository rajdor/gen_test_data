#!/bin/bash

echo "########################################################################"
echo "# LAST NAMES"
echo "########################################################################"
export inputDir=../00_rawdata/
export inputFilename=dist.all.last

export outputDir=./ref_last_names/
export outputFilename=ref_last_names.txt

echo "input   : ${inputDir}${inputFilename}"
echo "output  : ${outputDir}${outputFilename}"

echo "Resetting output Directory : ${outputDir}"
rm -rf ${outputDir}
mkdir ${outputDir}

echo "Copying ${inputDir}${inputFilename} to ${outputDir}${inputFilename}"
cp ${inputDir}${inputFilename} ${outputDir}${inputFilename}

echo "Preparing file format for lastname ${outputDir}${inputFilename}"
python3 preprocess_ref_last_names.py ${outputDir}${inputFilename} > ${outputDir}${outputFilename}

rm ${outputDir}${inputFilename}
ls -al ${outputDir}
head ${outputDir}${outputFilename}


echo "########################################################################"
echo "# Female NAMES"
echo "########################################################################"
export inputDir=../00_rawdata/
export inputFilename=dist.female.first

export outputDir=./ref_first_names/
export outputFilename=ref_female_names.txt
echo "input   : ${inputDir}${inputFilename}"
echo "output  : ${outputDir}${outputFilename}"

echo "Resetting output Directory : ${outputDir}"
rm -rf ${outputDir}
mkdir ${outputDir}

echo "Copying ${inputDir}${inputFilename} to ${outputDir}${inputFilename}"
cp ${inputDir}${inputFilename} ${outputDir}${inputFilename}

export startAt=0
echo "Preparing file format for lastname ${outputDir}${inputFilename}"
python3 preprocess_ref_first_names.py ${startAt} F ${outputDir}${inputFilename} > ${outputDir}${outputFilename}

rm ${outputDir}${inputFilename}
ls -al ${outputDir}
head ${outputDir}${outputFilename}


echo "########################################################################"
echo "# Male NAMES"
echo "########################################################################"
export inputDir=../00_rawdata/
export inputFilename=dist.male.first

export outputDir=./ref_first_names/
export outputFilename=ref_male_names.txt
echo "input   : ${inputDir}${inputFilename}"
echo "output  : ${outputDir}${outputFilename}"

echo "Copying ${inputDir}${inputFilename} to ${outputDir}${inputFilename}"
cp ${inputDir}${inputFilename} ${outputDir}${inputFilename}


export startAt=$(< "${outputDir}ref_female_names.txt" wc -l)
echo "Preparing file format for lastname ${outputDir}${inputFilename}"
python3 preprocess_ref_first_names.py ${startAt} M ${outputDir}${inputFilename} > ${outputDir}${outputFilename}

rm ${outputDir}${inputFilename}
ls -al ${outputDir}
head ${outputDir}${outputFilename}


echo "########################################################################"
echo "# Concatenating female and male names"
echo "########################################################################"
cat ${outputDir}ref_male_names.txt ${outputDir}ref_female_names.txt > ${outputDir}ref_first_names.txt 
rm ${outputDir}ref_male_names.txt ${outputDir}ref_female_names.txt
ls -al ${outputDir}
head ${outputDir}ref_first_names.txt 


