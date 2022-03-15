#!/bin/bash
apt-get update && apt-get install unixodbc-dev -y && apt-get install g++ -y && cd /mnt/geocode && pip install -r requirements.txt && python ./geocoder-10-threads-public.py
