import sys

import pymongo
from hyperopt import hp, fmin, tpe
from hyperopt.mongoexp import MongoTrials

from fondos.series import Quote, Portfolio, ds
from fondos import portfolio_selection

tickers = ['APBR', 'PAMP', 'YPFD', 'GGAL', 'ERAR', 'CRES', 'COME', 'ALUA', 'FRAN', 'MIRG',
           'BMA', 'TRAN', 'TS', 'JMIN', 'EDN', 'TGSU2', 'SAMI', 'AGRO', 'TECO2', 'PESA',
           'CEPU', 'CTIO', 'CECO2', 'AUSO', 'PETR', 'CELU', 'TGNO4']

# for t in tickers:
#     print Quote(t).data is None
Quote.fetch_all(tickers)

tickers = [e for e in tickers if Quote(e).data is not None]
tickers = [e for e in tickers if not Quote(e).data['2017-01-01':].empty]


def do_model_selection(exp_key):
    trials = MongoTrials('mongo://localhost:27017/merval/jobs', exp_key=exp_key)
    c = pymongo.MongoClient()
    coll = c.merval.jobs

    existing_jobs = coll.find({'exp_key': exp_key}).count()
    if existing_jobs > 0:
        print "There are %s jobs with this exp hey" % existing_jobs
        output = raw_input('[C]ontinue anyway, [D]elete them, [A]bort? [a/c/d] ').strip().lower()
        if output == 'd':
            output = raw_input('Sure? [y/N]').strip().lower()
            if output != 'y':
                print "bye"
                return
            else:
                print coll.delete_many({'exp_key': exp_key})
        elif output == 'a':
            print "bye"
            return
    print coll.delete_many({'result.status': 'new', 'book_time': None})

    space = portfolio_selection.portfolio_space(
        start_date='2017-01-01',
        end_date='2017-04-01',
        tickers=tickers,
        min_portfolio_size=3,
        max_portfolio_size=10
    )

    fmin(portfolio_selection.loss_function, space, tpe.suggest, 6000, trials=trials)


if __name__ == '__main__':
    exp_key = sys.argv[1]
    do_model_selection(exp_key)
