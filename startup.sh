#!/bin/bash
apt-get update && cd /mnt/geocode && pip install -r requirements.txt && python ./geocoder-10-threads-public.py
#this was added to force a permission change