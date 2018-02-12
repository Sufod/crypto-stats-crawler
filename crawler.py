#!/usr/bin/env python3
import urllib.request
import json
import csv
import mmap
import time
import argparse
import os

class Logger:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    def header(self,message):
        print(logger.HEADER +message+ logger.ENDC)
        
    def okblue(self,message):
        print(logger.OKBLUE +message+ logger.ENDC)
    
    def okgreen(self,message):
        print(logger.OKGREEN +message+ logger.ENDC)
    
    def warning(self,message):
        print(logger.WARNING +message+ logger.ENDC)
    
    def fail(self,message):
        print(logger.FAIL +message+ logger.ENDC)
    
    def bold(self,message):
        print(logger.BOLD+message+ logger.ENDC)
    
    def header(self,message):
        print(logger.UNDERLINE +message+ logger.ENDC)
    
    def header(self,message):
        print(logger.HEADER +message+ logger.ENDC)
    

def getLastLine(source):
    mapping = mmap.mmap(source.fileno(), 0, prot=mmap.PROT_READ)
    return mapping[mapping.rfind(b'\n', 0, -1) + 1:]


def initialize(file,originalTimestamp,writeHeaders=False):
    entries = getNewBatch(currency, originalTimestamp)['Data']
    logger.okblue("Fetched " + str(originalTimestamp) + " (" + str(len(entries)) + " entries)")
    writer = csv.DictWriter(file, fieldnames=FIELDS_NAMES)
    if writeHeaders:
        writer.writeheader()
    writer.writerows(entries)
    file.flush()

def getNewBatch(currency, from_timestamp, limit=1000):
    return json.loads(urllib.request.urlopen("https://min-api.cryptocompare.com/data/histominute?fsym="+currency+"&tsym=EUR&limit=" + str(
        limit) + "&e=CCCAGG&toTs=" + str(from_timestamp)).read().decode('utf-8'));

def coinList():
    return list(json.loads(urllib.request.urlopen("https://min-api.cryptocompare.com/data/all/coinlist").read().decode('utf-8'))['Data'].keys())


def writeRows(file, rows):
    writer = csv.DictWriter(file, fieldnames=FIELDS_NAMES)
    writer.writerows(rows)


def fetch_rows(timenow, lastLine):
    diff = timenow - int(lastLine)
    if diff > 60000:
        newLine = lastLine + 60000
        entries = getNewBatch(currency, newLine)['Data'][1:]
        logger.okblue("Fetched " + str(newLine) + " (" + str(len(entries)) + " entries)")
        writeRows(csvfile, entries)
        return newLine
    else:
        newLine = timenow
        entriesNumber = int((diff) / 60)
        if entriesNumber > 0:
            entries = getNewBatch(currency, newLine, entriesNumber)['Data'][1:]
            logger.okgreen("Fetched last batch " + str(newLine) + " (" + str(len(entries)) + " entries)")
            writeRows(csvfile, entries)
            return 0
        else:
            logger.okgreen("Up to date")
            return 0

DEFAULT_CURRENCY="BTCZ"
FIELDS_NAMES = ['time', 'high', 'low', 'open', 'volumefrom', 'volumeto', 'close']

# ---- Parsing args ----#
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--currency",
                    help="choose the cryptocurrency to crawl (default:" + DEFAULT_CURRENCY + ") more @ https://www.cryptocompare.com/api/data/coinlist/ ",
                    default=os.environ.get('DEFAULT_CURRENCY', DEFAULT_CURRENCY),
                    )

args = parser.parse_args()
timenow = int(time.time())
logger = Logger()
originalTimestamp = timenow-600000
currency=args.currency;
logger.header("=== Crypto Stats Crawler ===")
if currency not in coinList():
    logger.fail("Error, currency: "+ currency + " doesn't seems to exists...aborting")
    exit(-1)
filename = currency+"-latest.csv"

with open(filename, 'a+', newline='') as csvfile:
    if os.path.getsize(filename) == 0:
        initialize(csvfile,originalTimestamp,True)

    lastLine = getLastLine(csvfile).decode('UTF-8').split(",")[0]

    if lastLine == "time":
        initialize(csvfile,originalTimestamp)
        lastLine = getLastLine(csvfile).decode('UTF-8').split(",")[0]

    newLine = lastLine
    while newLine != 0:
        newLine = fetch_rows(timenow, int(newLine))
