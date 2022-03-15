#!/bin/bash
apt-get update &&
apt-get install unixodbc-dev -y &&
apt-get install g++ -y &&
curl https://packages.microsoft.com/keys/microsoft.asc &&
apt-key add -y &&
curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list &&
apt-get update &&
ACCEPT_EULA=Y apt-get install -y msodbcsql18 &&
cd /mnt/geocode &&
pip install -r requirements.txt &&
python ./geocoder-10-threads-public.py
