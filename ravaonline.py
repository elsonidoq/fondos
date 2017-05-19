import csv
import os
import re
from StringIO import StringIO
from datetime import datetime
from urllib2 import urlopen

import pandas as pd
from fito.data_store.file import FileDataStore
from fito.operations.decorate import as_operation
from lxml.html import document_fromstring

here = os.path.abspath(os.path.dirname(__file__))

cache = FileDataStore(os.path.join(here, 'ravaonline'))


@as_operation(cache_on=cache)
def get_raw_data(symbol):
    url = 'http://www.ravaonline.com/v2/empresas/perfil.php?h=max&e={}'.format(symbol)
    content = urlopen(url).read()
    return content


@as_operation(cache_on=cache)
def get_prices(symbol):
    url = 'http://www.ravaonline.com/v2/empresas/precioshistoricos.php?e={}&csv=1'.format(symbol)
    content = urlopen(url).read()
    data = list(csv.DictReader(StringIO(content)))

    df = pd.DataFrame(data)
    df.fecha = pd.to_datetime(df.fecha, format='%Y-%m-%d')
    df = df.set_index('fecha')

    return df


def get_dividends(symbol):
    dom = document_fromstring(get_raw_data(symbol).execute())

    rows = dom.cssselect('.cabecerapanel')
    for row in rows:
        if u'Mar Jun Sep DicTot' in row.text_content():
            break
    else:
        return

    tab = row.getparent()
    rows = tab.cssselect('tr')
    header = [
        (
            re.sub(
                '\W',
                '',
                e.text_content(),
                flags=re.U
            )
                .strip()
                .lower()
        )
        for e in rows[2].cssselect('td')
        ]
    header = [e for e in header if e]
    docs = []
    for row in rows[3:]:
        row = [
            (
                e
                    .text_content()
                    .strip()
                    .replace('I', '')
                    .replace('V', '')
                    .replace(' e', '')
                    .replace(',', '.')
            )
            for e in row
            ]
        row = [float(e) for e in row if e if e != '-']
        docs.append(
            dict(
                zip(
                    header,
                    row
                )
            )
        )

    l = []
    months = {
        'dic': 12, 'jun': 6, 'sep': 9, 'mar': 3
    }
    for doc in docs:
        year = int(doc.pop(u'a\xf1o'))
        for k, v in doc.iteritems():
            if k not in months: continue
            month = months[k]
            l.append(
                {
                    'date': datetime(year, month, 1),
                    'value': v
                }
            )

    return pd.DataFrame(l).set_index('date')


@cache.autosave()
def get_symbols():
    urls = (
        'http://www.ravaonline.com/v2/empresas/bonos.php',
        'http://ravaonline.com/v2/precios/panel.php?m=LID',
        'http://ravaonline.com/v2/precios/panel.php?m=GEN',
        'http://ravaonline.com/v2/precios/panel.php?m=OPC',
        'http://ravaonline.com/v2/precios/panel.php?m=BON',
        'http://ravaonline.com/v2/precios/panel.php?m=ADRARG',
        'http://ravaonline.com/v2/precios/panel.php?m=LEBAC',
        'http://ravaonline.com/v2/precios/panel.php?m=GLOBAL',
        'http://ravaonline.com/v2/precios/panel.php?m=DOW30',
        'http://ravaonline.com/v2/precios/panel.php?m=ETF',
        'http://ravaonline.com/v2/precios/panel.php?m=VARIAS'
    )

    res = []

    for url in urls:
        content = urlopen(url).read()
        dom = document_fromstring(content)
        for a in dom.cssselect('a'):
            href = a.attrib.get('href', '')
            parts = href.split('?')
            if parts[0].endswith('/v2/empresas/perfil.php') and len(parts) == 2:
                res.append(parts[1].split('=')[1])

    return res
