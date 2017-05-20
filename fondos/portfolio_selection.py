import numpy as np
from pprint import pprint

from hyperopt import STATUS_OK
from hyperopt import hp
from hyperopt.pyll import scope

from series import Quote, Portfolio


@scope.define
def normalize(weights):
    total = float(sum(weights))
    return [w / total for w in weights]


def portfolio_space(start_date, end_date, tickers, min_portfolio_size, max_portfolio_size):
    portfolio_options = []
    for portfolio_size in xrange(min_portfolio_size, max_portfolio_size + 1):
        indices = []
        weights = []
        for ticker_index in xrange(portfolio_size):
            indices.append(
                hp.quniform(
                    'portfolio_{}_ticker_{}'.format(portfolio_size, ticker_index),
                    0,
                    len(tickers) - ticker_index - 1,
                    1
                ),
            )

            weights.append(
                hp.uniform(
                    'weight_portfolio_{}_ticker_{}'.format(portfolio_size, ticker_index),
                    0,
                    1
                )

            )

        portfolio_options.append(
            {
                'indices': indices,
                'weights': scope.normalize(weights)
            }
        )

    return {
        'tickers'   : tickers,
        'start_date': start_date,
        'end_date'  : end_date,
        'portfolio' : hp.choice('portfolio', portfolio_options)
    }


def loss_function(params):
    pprint(params)

    tickers = list(params['tickers'])
    quotes = []
    weights = []
    for i, ticker_index in enumerate(params['portfolio']['indices']):
        quote = Quote(tickers.pop(int(ticker_index)))
        quotes.append(quote)
        weights.append(params['portfolio']['weights'][i])

    portfolio = Portfolio(quotes, weights)
    res = portfolio.loss(
        start_date=params['start_date'],
        end_date=params['end_date']
    )
    res['status'] = STATUS_OK
    pprint(res)
    return res
