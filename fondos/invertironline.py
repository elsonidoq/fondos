import unicodedata
import re
import os
from datetime import datetime
from urllib2 import urlopen

from fito import Operation
from fito import PrimitiveField
from fito.data_store.file import FileDataStore, RawSerializer
from lxml.html import document_fromstring

here = os.path.abspath(os.path.dirname(__file__))


class InvertirOnlineDownloader(Operation):
    bond_name = PrimitiveField(0)
    timestamp = PrimitiveField(default=None)

    default_data_store = FileDataStore(
        os.path.join(here, 'invertir_online'),
        serializer=RawSerializer(),
        get_cache_size=1000,
    )

    def __init__(self, *args, **kwargs):
        super(InvertirOnlineDownloader, self).__init__(*args, **kwargs)
        self.timestamp = self.timestamp or datetime.now()

    def apply(self, runner):
        url = 'https://www.invertironline.com/titulo/cotizacion/bcba/{}'.format(self.bond_name)
        return urlopen(url).read()

    @property
    def data(self):
        return parse_table(self.execute())


def strip_accents(s):
    if not isinstance(s, unicode): s = s.decode('utf8')
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def extract_string(elem):
    res = elem.text_content()

    if isinstance(res, unicode):
        res = unicode(res)
    else:
        res = str(res)

    return res


def parse_table(html):
    np = re.compile('^\d+(\.\d+)$', re.U)
    sp = re.compile('\s+', re.U)

    dom = document_fromstring(html)
    table = dom.cssselect('#tabs-1')[0].cssselect('table')[0]

    rows = table.cssselect('tr')[1:]

    res = {}
    for row in rows:
        tds = row.cssselect('td')
        ks = [td for i, td in enumerate(tds) if i % 2 == 0]
        vs = [td for i, td in enumerate(tds) if i % 2 == 1]

        for i, (k, v) in enumerate(zip(ks, vs)):
            k = (
                sp.sub(' ',
                       strip_accents(
                           extract_string(k)
                       )
                       .replace('\n', ' ')
                       )
                    .replace(':', '')
                    .replace('.', '')
                    .strip()
                    .replace(' ', '_')
                    .lower()
            )
            v = (
                v
                    .text_content()
                    .replace('$', '')
                    .replace('%', '')
                    .replace('.', '')
                    .replace(',', '.')
                    .strip()
            )
            if np.match(v): v = float(v)

            res[k] = v

    return res
