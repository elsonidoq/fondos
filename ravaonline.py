import json
import os
import re
from urllib2 import urlopen

import pandas as pd
from fito.data_store.file import FileDataStore
from lxml.html import document_fromstring

here = os.path.abspath(os.path.dirname(__file__))

cache = FileDataStore(os.path.join(here, 'ravaonline'))


@cache.autosave()
def get_data(symbol):
    url = 'http://www.ravaonline.com/v2/empresas/perfil.php?e={}'.format(symbol)
    content = urlopen(url).read()
    p = re.compile('chartData.push\((?P<json>.*?)\)')
    res = []
    for match in p.finditer(content):
        res.append(
            json.loads(
                match
                .groupdict()['json']
                .replace("'", '"')
            )
        )

    return res


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


def get_dataframe(symbol):
    data = get_data(symbol)

    df = pd.DataFrame(data)
    df.date = pd.to_datetime(df.date, format='%Y-%m-%d')
    df = df.set_index('date')

    return df






