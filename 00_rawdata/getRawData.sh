#!/bin/bash

echo "#####################################################################"
echo "Getting color file from internets ~15MB"
echo "#####################################################################"
wget https://colornames.org/download/colornames.zip

echo "#####################################################################"
echo "Getting names files from internets ~3.0MB"
echo "#####################################################################"
wget http://www2.census.gov/topics/genealogy/1990surnames/dist.all.last
wget http://www2.census.gov/topics/genealogy/1990surnames/dist.female.first
wget http://www2.census.gov/topics/genealogy/1990surnames/dist.male.first

echo "#####################################################################"
echo "Getting vehicle raw file from internets ~1.5MB"
echo "#####################################################################"
wget https://fueleconomy.gov/feg/epadata/vehicles.csv.zip

echo "#####################################################################"
echo "Getting address raw file from internets ~1.4GB)"
echo "#####################################################################"
wget https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/feb20_gnaf_pipeseparatedvalue.zip 
