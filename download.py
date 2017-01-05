import os
import traceback
from urllib2 import urlopen

from datetime import datetime
from fito.data_store.file import FileDataStore

here = os.path.abspath(os.path.dirname(__file__))

ds = FileDataStore(os.path.join(here, 'downloads'))

@ds.cache()
def download(datetime):
    url = 'http://www.santanderrio.com.ar/ConectorPortalStore/Rendimiento'
    return urlopen(url).read()


if __name__ == '__main__':
    try:
        now = datetime.now()
        print "downloading for {}...".format(now)
        download(now)
    except Exception, e:
        traceback.print_exc()
        print
