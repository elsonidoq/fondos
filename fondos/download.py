import os

from datetime import datetime

from invertironline import InvertirOnlineDownloader

here = os.path.abspath(os.path.dirname(__file__))


def main():
    bonds = [
        'aa17',
        'ay24'
    ]

    print "Downloading {}".format(datetime.now())
    for bond in bonds:
        InvertirOnlineDownloader(bond).execute()


if __name__ == '__main__':
    main()
