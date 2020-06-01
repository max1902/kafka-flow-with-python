import aiohttp
import asyncio
from urllib.parse import (
    urlencode,
    urlsplit,
    urlunsplit,
)
from bs4 import BeautifulSoup
from kafka import KafkaProducer

main_url = 'https://auto.ria.com/search/'


def prepare_url(page=0):
    scheme, netloc, path, *_ = urlsplit(main_url)
    new_query = urlencode({
        'year[0].gte': 2010,
        'price.currency': '1',
        'gearbox.id[1]': '2',
        'gearbox.id[2]': '3',
        'gearbox.id[3]': '4',
        'gearbox.id[4]': '5',
        'fuel.id[1]': '2',
        'mileage.lte': '100',
        'abroad.not': '0',
        'custom.not': '1',
        'page': page,
        'size': 100,
        'sellerType': '1',
    })
    return urlunsplit((scheme, netloc, path, new_query, None))


def publish_messages(cars_pages, key):
    def on_send_success(record_metadata):
        success_msg = f'Message successfully published to {record_metadata.topic}'
        msg_size = record_metadata.serialized_value_size
        print(f'{success_msg}. Message size is {msg_size}')

    kafka_cli = kafka_producer_cli()
    for page in cars_pages:
        key = key.encode()
        value = page.strip().encode()
        kafka_cli.send('raw_cars_pages', key=key, value=value).add_callback(on_send_success)
    kafka_cli.flush()


def kafka_producer_cli():
    return KafkaProducer(bootstrap_servers=['localhost:9092'])


async def fetch_raw(session, url):
    async with session.get(url) as response:
        return await response.text()


async def parse_page(session, response):
    raw_cars_task = []
    soup = BeautifulSoup(response, 'lxml')
    links = soup.select('section.ticket-item.paid')
    for link in links:
        car_url = link.select_one('a.m-link-ticket')['href']
        raw_cars_task.append(fetch_raw(session, car_url))
    if not raw_cars_task:
        return None

    raw_pages = await asyncio.gather(*raw_cars_task)
    return raw_pages


async def fetch_main(session, url):
    async with session.get(url) as response:
        raw_resp = await response.text()
        cars_pages = await parse_page(session, raw_resp)
        if cars_pages:
            publish_messages(cars_pages, 'raw')


def create_tasks(session, max_page=0):
    tasks = []
    for page in range(max_page):
        url = prepare_url(page)
        task = fetch_main(session, url)
        tasks.append(task)
    return tasks


async def main(max_page=0):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*create_tasks(session, max_page=max_page))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(max_page=10))
