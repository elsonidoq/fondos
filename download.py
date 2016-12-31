import os
import traceback
from time import sleep
from urllib2 import urlopen

from datetime import datetime
from fito.data_store.file import FileDataStore

here = os.path.abspath(os.path.dirname(__file__))

ds = FileDataStore(os.path.join(here, 'downloads'))

@ds.cache()
def download(datetime):
    url = 'http://www.santanderrio.com.ar/banco/online/personas/inversiones/super-fondos/rendimientos'
    return urlopen(url).read()


if __name__ == '__main__':
    while True:
        try:
            download(datetime.now())
            sleep(1)
        except Exception, e:
            traceback.print_exc()
            print
