import csv
import gzip
from datetime import datetime

from fito import Operation
from fito import PrimitiveField
from fito import SpecField
from fito.data_store.file import RawSerializer, FileDataStore
from lxml.html import document_fromstring

import urllib2
import re

float_pattern = re.compile('^\d+\.\d+$')


class CompressedStreamSerializer(RawSerializer):
    def save(self, stream, subdir):
        with gzip.open(self.get_fname(subdir), 'w') as f:
            while True:
                bytes = stream.read(10240)
                if not bytes: return
                f.write(bytes)

    def load(self, subdir):
        return gzip.open(self.get_fname(subdir))


dumps_store = FileDataStore('quandl/dumps_store', serializer=CompressedStreamSerializer())
cache = FileDataStore('quandl/cache')


def get_last_dump_timestamp():
    return max(dumps_store.iterkeys(), key=lambda x: x.timestamp).timestamp


def get_api_key():
    with open('quandl_api_key') as f:
        return f.read()


class RawData(Operation):
    timestamp = PrimitiveField(0)
    default_data_store = dumps_store

    @classmethod
    def ensure_recent(cls, max_age=7):
        now = datetime.now()
        if dumps_store.is_empty() or (now - get_last_dump_timestamp()).days > max_age:
            res = RawData(now)
            res.dump()
            return res

    def apply(self, runner):
        url = 'https://www.quandl.com/api/v3/datatables/WIKI/PRICES?qopts.export=true&api_key={}'.format(get_api_key())
        raw = urllib2.urlopen(url).read()
        dom = document_fromstring(raw)
        download_link = dom.cssselect('.download-button')[0].attrib['href']
        return urllib2.urlopen(download_link)


class MongoData(Operation):
    raw_data = SpecField(0, base_type=RawData)
    default_data_store = cache

    def apply(self, runner):
        stream = runner.execute(self.raw_data)

        reader = csv.DictReader(stream)
        for doc in reader:
            for k, v in doc.iteritems():
                if v.isdigit():
                    doc[k] = int(v)
                elif is_float(v):
                    doc[k] = float(v)

            doc['date'] = datetime.strptime(doc['date'], '%Y-%m-%d')

        stream.close()


def is_float(str):
    return float_pattern.match(str) is not None
