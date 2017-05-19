from threading import Thread
import os

import numpy as np
from collections import namedtuple

from fito.specs.fields import CollectionField, SpecCollection, KwargsField
from pandas_datareader import data, wb

from fito import PrimitiveField
from fito import Spec
from fito import as_operation
from fito.data_store import FileDataStore

here = os.path.dirname(__file__)
ds = FileDataStore(
    os.path.abspath(os.path.join(here,'caches')),
    get_cache_size=1000
)

Series = namedtuple('Series', 'source data'.split())


class Quote(Spec):
    ticker = PrimitiveField(0)

    @as_operation(cache_on=ds, method_type='instance')
    def get_data(self):
        print "Getting {}".format(self.ticker)
        for method in 'get_data_google get_data_yahoo'.split():
            try:
                return Series(
                    source=method,
                    data=getattr(data, method)(self.ticker)
                )
            except Exception:
                continue

    @property
    def data(self):
        series = self.get_data().execute()
        if series is not None: return series.data

    def get_returns(self, field='close', start_date=None):
        series = self.data[field.capitalize()]
        if start_date is not None: series = series[start_date:]
        return series / series.iloc[0]

    @classmethod
    def fetch_all(cls, tickers):
        quotes = map(cls, tickers)
        ts = []
        for q in quotes:
            t = Thread(target=q.get_data().execute)
            t.daemon = True
            t.start()

        for t in ts:
            t.join()

        print "Done!"


class Portfolio(Spec):
    quotes = SpecCollection(0)
    weights = CollectionField(1)

    def __init__(self, *args, **kwargs):
        super(Portfolio, self).__init__(*args, **kwargs)
        assert len(self.quotes) == len(self.weights)
        total = sum(self.weights)
        if abs(total - 1) > 0.000001:
            raise RuntimeError('Invalid portfolio, sum(self.quotes.values()) == {}'.format(total))

    def get_returns(self, field='close', start_date=None):
        res = 0
        for i, quote in enumerate(self.quotes):
            res += self.weights[i] * quote.get_returns(field, start_date)
        return res

    def get_volatility(self, field='close', start_date=None):
        iterator = enumerate(self.quotes)
        i, quote = iterator.next()
        series = self.weights[i] * quote.data[field.capitalize()]

        for i, quote in iterator:
            series += self.weights[i] * quote.data[field.capitalize()]

        if start_date is not None:
            series = series[start_date:]

        logreturns = np.log(series / series.shift(1))
        return np.sqrt(252 * logreturns.var())
