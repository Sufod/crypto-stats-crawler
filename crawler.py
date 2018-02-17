#!/usr/bin/env python3
import urllib.request
import json
import csv
import mmap
import time
import argparse
import os

from Logger import Logger


class CryptoCrawler:

    def __init__(self, currency):
        self._currency = currency
        self.current_timestamp = int(time.time())
        self.oldest_crawlable_timestamp = self.current_timestamp - 600000
        self.fields_names = ['time', 'high', 'low', 'open', 'volumefrom', 'volumeto', 'close']
        self.coin_list = self.fetch_coin_list()

    def get_last_line(self, source):
        mapping = mmap.mmap(source.fileno(), 0, prot=mmap.PROT_READ)
        return mapping[mapping.rfind(b'\n', 0, -1) + 1:]

    def initialize(self, file, currency, write_headers=False):
        entries = self.fetch_new_batch(currency, self.oldest_crawlable_timestamp)['Data']
        Logger.okblue("Fetched {0} ({1} entries)".format(str(self.oldest_crawlable_timestamp), str(len(entries))))
        writer = csv.DictWriter(file, fieldnames=self.fields_names)
        if write_headers:
            writer.writeheader()
        writer.writerows(entries)
        file.flush()

    def fetch_new_batch(self, currency, from_timestamp, limit=1000):
        return json.loads(urllib.request.urlopen(
            "https://min-api.cryptocompare.com/data/histominute?fsym=" + currency + "&tsym=EUR&limit=" + str(
                limit) + "&e=CCCAGG&toTs=" + str(from_timestamp)).read().decode('utf-8'));

    def fetch_coin_list(self):
        return list(json.loads(
            urllib.request.urlopen("https://min-api.cryptocompare.com/data/all/coinlist").read().decode('utf-8'))[
                        'Data'].keys())

    def write_rows(self, file, rows):
        writer = csv.DictWriter(file, fieldnames=self.fields_names)
        writer.writerows(rows)

    def fetch_and_write_rows(self, currency, csvfile, last_line):
        diff = self.current_timestamp - int(last_line)
        if diff > 60000:
            new_line = last_line + 60000
            entries = self.fetch_new_batch(currency, new_line)['Data'][1:]
            Logger.okblue("Fetched {0} ({1} entries)".format(str(new_line), str(len(entries))))
            self.write_rows(csvfile, entries)
            return new_line
        else:
            new_line = self.current_timestamp
            entries_number = int(diff / 60)
            if entries_number > 0:
                entries = self.fetch_new_batch(currency, new_line, entries_number)['Data'][1:]
                Logger.okgreen("Fetched last batch {0} ({1} entries)".format(str(new_line), str(len(entries))))
                self.write_rows(csvfile, entries)
                return 0
            else:
                Logger.okgreen("Up to date")
                return 0

    def run(self):
        Logger.bold("=== Crypto Stats Crawler ===")
        # Handle multi currency here
        for currency in self._currency:
            Logger.header("== Crawling " + currency + " ==")

            if currency not in self.fetch_coin_list():
                Logger.fail("Error, currency: " + currency + " doesn't seems to exists...aborting")
                continue
            filename = currency + "-latest.csv"

            with open(filename, 'a+', newline='') as csvfile:
                if os.path.getsize(filename) == 0:
                    self.initialize(csvfile, currency, True)

                last_line = self.get_last_line(csvfile).decode('UTF-8').split(",")[0]

                if last_line == "time":
                    self.initialize(csvfile, currency)
                    last_line = self.get_last_line(csvfile).decode('UTF-8').split(",")[0]

                new_line = last_line
                while new_line != 0:
                    new_line = self.fetch_and_write_rows(currency, csvfile, int(new_line))


def main():
    DEFAULT_CURRENCY = ["BTCZ"]
    # ---- Parsing args ----#
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--currency",
                        nargs='+',
                        help="choose the cryptocurrency to crawl (default:" + DEFAULT_CURRENCY[
                            0] + ") more @ https://www.cryptocompare.com/api/data/coinlist/ ",
                        default=os.environ.get('DEFAULT_CURRENCY', DEFAULT_CURRENCY),
                        )

    args = parser.parse_args()

    crawler = CryptoCrawler(args.currency)
    crawler.run()


if __name__ == "__main__": main()
