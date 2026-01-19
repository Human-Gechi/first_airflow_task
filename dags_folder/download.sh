#!/bin/bash
# date in YYYYMMDD
today=$(date +%Y%m%d)
today=20251201

# current hour in 24 hours format
hour=$(date +%H)
hour=22

# hour with minutes/seconds as needed
time="${hour}0000"
time=220000

# url
url="https://dumps.wikimedia.org/other/pageviews/2025/2025-12/pageviews-${today}-${time}.gz"
# Download to /tmp/ tmeporay folder
curl -L "${url}" -o /tmp/wikipedia_pageviews.gz