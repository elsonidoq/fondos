from lxml.html import document_fromstring
import re


def parse_table(html):
    negative_pat = re.compile('\(.*?\)')

    dom = document_fromstring(html)
    rows = dom.cssselect('tr')

    data_rows = rows[4:]
    fields = ['name', 'price', 'daily_variation', 'monthly_variation', 'quarterly_variation', 'yearly_variation']
    for row in data_rows:
        tds = row.xpath('td')
        if len(tds) != len(fields): continue

        contents = [td.text_content().strip() for td in tds]
        for i, val in enumerate(contents):
            if i == 0: continue
            val = val.replace('.', '').replace(',', '.')
            if negative_pat.match(val):
                val = -float(val[1:-1])
            else:
                val = float(val)
            contents[i] = val


        yield dict(zip(fields, contents))
