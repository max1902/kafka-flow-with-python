import json
import locale

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


def publish_messages(pages, topic, rkey):
    def on_send_success(record_metadata):
        success_msg = f'Message successfully published to {record_metadata.topic}'
        msg_size = record_metadata.serialized_value_size
        print(f'{success_msg}. Message size is {msg_size}')

    kafka_cli = kafka_producer_cli()
    for page in pages:
        key = rkey.encode()
        value = page.strip().encode()
        kafka_cli.send(topic, key=key, value=value).add_callback(on_send_success)
    kafka_cli.flush()


def kafka_producer_cli():
    return KafkaProducer(bootstrap_servers=['localhost:9092'])


def parse_price(price_block):
    price_dict = {}
    usd_price = price_block.select_one('div.price_value strong')
    if not usd_price:
        usd_price = price_block.select_one('div.price_value')
    usd_price = usd_price.text.strip()
    price_dict['USD'] = locale.atoi(usd_price.replace('$', '').strip().replace(' ', ','))
    price_other = price_block.select_one('div.price_value--additional')
    if price_other:
        price_eur_tag = price_other.select_one('span[data-currency=EUR]')
        if price_eur_tag:
            price_dict['EUR'] = price_eur_tag.text
        price_uah_tag = price_other.select_one('span[data-currency=UAH]')
        if price_uah_tag:
            price_dict['UAH'] = price_uah_tag.text
    return price_dict


def parse_tech_info(tech_info_body):
    info = {}
    keys = {
        'Пробег': 'Kilometer',
        'Двигатель': 'Fuel Type',
        'Коробка передач': 'Transmission',
        'Привод': 'Wheel Drive',
        'Цвет': 'Color',
    }
    for tag in tech_info_body.select('dd'):
        title = tag.select_one('span.label')
        if not title or not title.text:
            continue

        title = title.text.strip()
        if title in keys:
            info[keys[title]] = tag.select_one('span.argument').text.strip()
    return info


def title_info(title_block):
    info = {'title': title_block.get('title')}
    brand_tag = title_block.select_one('span[itemprop=brand]')
    if brand_tag:
        info['brand'] = brand_tag.text
    series_tag = title_block.select_one('span[itemprop=name]')
    if series_tag:
        info['series'] = series_tag.text
    return info


def parse(raw_html):
    rec = {}
    soup = BeautifulSoup(raw_html.decode('utf-8'), 'lxml')
    title_block = soup.select_one('h1.head') or soup.select_one('h1.auto-head_title')
    if title_block:
        rec.update(title_info(title_block))
    rec.update(parse_price(soup))
    tech_info_body = soup.select_one('div.description-car')
    if tech_info_body:
        tech_info_res = parse_tech_info(tech_info_body)
        if tech_info_res:
            rec.update(tech_info_res)
    seller_tag = soup.select_one('div.seller_info_area h4.seller_info_name')
    if seller_tag:
        rec['seller'] = seller_tag.text.strip()
    phone_tag = soup.select_one('div.phones_item span')
    if phone_tag:
        rec['phone'] = phone_tag.get('data-phone-number')
    return json.dumps(rec)


if __name__ == '__main__':
    topic_name = 'raw_cars_pages1'
    consumer = KafkaConsumer(
        topic_name,
        auto_offset_reset='earliest',
        bootstrap_servers=['localhost:9092'],
        consumer_timeout_ms=1000,
    )
    parsed_pages = [parse(msg.value) for msg in consumer]
    consumer.close()
    if len(parsed_pages) > 0:
        print('Publishing records..')
        publish_messages(parsed_pages, 'parsed_cars_pages1', 'parsed')
