import requests
from bs4 import BeautifulSoup
import boto3
import json

def scrape_aruodas(event, context):
    page = requests.get('https://en.aruodas.lt/butai/vilniuje/puslapis/2/')
    soup = BeautifulSoup(page.content, 'html.parser')
    s3 = boto3.resource('s3', region_name='eu-west-2')

    for row in soup.select('tr.list-row td.list-adress h3 a'):
        place = {'website': 'aruodas'}
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
        }

        print(row.attrs['href'])

        single_page = requests.get(row.attrs['href'], headers=headers)
        single_soup = BeautifulSoup(single_page.content, 'html.parser')
        name = single_soup.select('h1.obj-header-text')[0]
        place['name'] = name.text.strip()

        stats = single_soup.select('div.obj-stats dl dd')
        place['href'] = stats[0].text.strip()

        path = place['href'].split('lt/')[1].split('-')
        place['id'] = path[1]

        if path[0] == '1':
            place['house'] = False
        else:
            place['house'] = True

        place['created_at'] = stats[1].text.strip()
        place['updated_at'] = stats[2].text.strip()

        price = single_soup.select('.price-block .price-left .price-eur')[0].text.strip()
        place['price'] = int(price.replace(' ', '').replace('€', ''))

        for r in single_soup.select('.obj-details dt'):
            rt = r.text.strip()
            if rt == 'Plotas:':
                area = r.find_next().text.strip()
                place['area'] = float(area.replace(' m²', '').replace(',', '.'))
            if rt == 'Kambarių sk.:':
                place['rooms'] = int(r.find_next().text.strip())
            if rt == 'Metai:':
                place['year'] = int(r.find_next().text.strip())
            if rt == 'Įrengimas:':
                place['equipment'] = r.find_next().contents[0].strip()

        map_url = 'https://www.aruodas.lt/map/?id=' + place['href'].split('lt/')[1] + '&position=popup'
        map_page = requests.get(map_url, headers=headers)
        map_soup = BeautifulSoup(map_page.content, 'html.parser')

        for line in map_soup.prettify().splitlines():
            if 'var locationCoordinate =' in line:
                location = line.split('var locationCoordinate = ')[1].replace("'", '').replace(';', '').split(',')
                place['lat'] = float(location[0])
                place['lng'] = float(location[1])

        s3.Object('chum-buket', place['id'] + ".json").put(Body=json.dumps(place))

    return {
        'message': 'Scrape those houses!',
        'event': event
    }


scrape_aruodas(1, 1)
