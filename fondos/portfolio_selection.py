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


def portfolio_space(start_date, end_date, tickers, max_portfolio_size):
    portfolio_options = []
    for portfolio_size in xrange(1, max_portfolio_size + 1):
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
        'tickers': tickers,
        'start_date': start_date,
        'end_date': end_date,
        'portfolio': hp.choice('portfolio', portfolio_options)
    }


def loss_function(params):
    pprint(params)

    tickers = list(params['tickers'])
    quotes = []
    weights = []
    for i, ticker_index in enumerate(params['portfolio']['indices']):
        try:
            quote = Quote(tickers.pop(int(ticker_index)))
        except: import ipdb;ipdb.set_trace()
        quotes.append(quote)
        weights.append(params['portfolio']['weights'][i])

    portfolio = Portfolio(quotes, weights)

    start_date = params['start_date']
    portfolio_return = portfolio.get_returns(start_date=start_date)[params['end_date']:].dropna().iloc[0]
    portfolio_volatility = portfolio.get_volatility(start_date=start_date)
    loss = - (portfolio_return - 0.05 * portfolio_volatility)
    if np.isnan(loss): import ipdb;ipdb.set_trace()
    print "Loss: {:<3}".format(loss)
    return dict(
        loss=loss,
        status=STATUS_OK,
        portfolio=portfolio.to_dict(),
    )


