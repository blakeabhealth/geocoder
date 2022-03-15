#!/bin/bash
apt-get update &&
apt-get install unixodbc-dev -y &&
apt-get install g++ -y &&
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - &&
curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list &&
apt-get update &&
ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y msodbcsql18  &&
ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y mssql-tools18 &&
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc &&
source ~/.bashrc &&
apt-get install -y unixodbc-dev &&
cd /mnt/geocode &&
pip install -r requirements.txt &&
python ./geocoder-10-threads-public.py
