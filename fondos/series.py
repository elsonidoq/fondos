import cPickle as pickle
from threading import Thread
import os
from fito import DictDataStore
from fito.data_store.mongo import MongoHashMap

import numpy as np
from collections import namedtuple

from fito.specs.fields import CollectionField, SpecCollection, KwargsField
from pandas_datareader import data, wb

from fito import PrimitiveField
from fito import Spec
from fito import as_operation
from fito.data_store import FileDataStore
from statsmodels.tsa.filters.hp_filter import hpfilter
from fondos import ravaonline

here = os.path.dirname(__file__)
ds = MongoHashMap(
    'fondos.stocks_cache',
    get_cache_size=1000,
    execute_cache_size=1000,
    use_gridfs=True
)

Series = namedtuple('Series', 'source data'.split())

mem_cache = DictDataStore()


class Quote(Spec):
    ticker = PrimitiveField(0)

    @as_operation(cache_on=ds, method_type='instance')
    def get_data(self):
        print "Getting {}".format(self.ticker)
        try:
            res = ravaonline.get_prices(self.ticker).execute()
            res['Close'] = res.cierre.apply(float)
            return pickle.dumps(
                Series(
                    source='ravaonline',
                    data=res,
                )
            )
        except Exception:
            pass

        for method in 'get_data_google get_data_yahoo'.split():
            try:
                return pickle.dumps(
                    Series(
                        source=method,
                        data=getattr(data, method)(self.ticker)
                    )
                )
            except Exception:
                continue

    @property
    def data(self):
        series = pickle.loads(self.get_data().execute())
        if series is not None: return series.data

    @mem_cache.autosave(method_type='instance')
    def get_returns(self, field='close', start_date=None, end_date=None):
        series = self.data[field.capitalize()]
        if start_date is not None: series = series[start_date:]

        res = series / series.iloc[0]

        if end_date is not None: res = res[:end_date]
        res.name = self.ticker
        return res

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

    def get_returns(self, field='close', start_date=None, end_date=None):
        res = 0
        for i, quote in enumerate(self.quotes):
            res += self.weights[i] * quote.get_returns(field, start_date, end_date)
        return res

    def get_volatility(self, field='close', start_date=None, end_date=None):
        iterator = enumerate(self.quotes)
        i, quote = iterator.next()
        series = self.weights[i] * quote.data[field.capitalize()]

        for i, quote in iterator:
            series += self.weights[i] * quote.data[field.capitalize()]

        if start_date is not None:
            series = series[start_date:]

        if end_date is not None:
            series = series[:end_date]

        logreturns = np.log(series / series.shift(1))
        return np.sqrt(252 * logreturns.var())

    def loss(self, start_date=None, end_date=None, verbose=True):
        portfolio_return = self.get_returns(
            start_date=start_date,
            end_date=end_date
        ).dropna().iloc[-1]

        portfolio_volatility = self.get_volatility(
            start_date=start_date,
            end_date=end_date
        )

        weights_too_imbalanced = max(0, (0.5 - min(self.weights) * len(self.weights)))

        returns = [
            hpfilter(quote.get_returns(start_date=start_date, end_date=end_date), 160)[1]
            for quote in self.quotes
        ]
        pairwise_correlations = []
        for i, r1 in enumerate(returns):
            for r2 in returns[i+1:]:
                pairwise_correlations.append(abs(r1.corr(r2)))

        loss = (
            - portfolio_return
            + 0.05 * portfolio_volatility
            + weights_too_imbalanced
            + 0.2 * np.max(pairwise_correlations)
        )

        if not verbose:
            return loss
        else:
            return dict(
                loss=loss,
                portfolio_return=portfolio_return,
                portfolio_volatility=portfolio_volatility,
                portfolio=self.to_dict(),
                max_correlations=np.max(pairwise_correlations)
            )

