## Kafka flow with Python
Produce raw htmls into Kafka, consume them, preprare json data and send to different topic. Then consume, filter and save DB.

## Installation
Install [kafka](https://kafka.apache.org/downloads) locally. Apache Kafka required Java to be installed too.

```bash
# run Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
# run Kafka broker(from new terminal)
bin/kafka-server-start.sh config/server.properties
# create topics(two in our case)
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_name
```

Install required Python libs.
```bash
pip install requirements.txt
```


## Usage

* ```produce_raw_html.py``` - collect and publish raw htmls to Kafka
* ```consume_produce_parsed_html.py``` - consume, parse and publish json data to Kafka
* ```consume_analyze.py``` - consume json data and send to Postgres
```bash
                         title                         |   brand    |         series         |  usd  |      phone      
-------------------------------------------------------+------------+------------------------+-------+-----------------
 Toyota Land Cruiser Prado 150 EXECUTIVE EDITION  2020 | Toyota     | Land Cruiser Prado 150 | 53500 | (098) 482 46 **
 BMW 520 2016                                          | BMW        | 520                    | 43000 | (098) 031 48 **
 JCB 531-70 AGRI 2012                                  | JCB        | 531-70                 | 41000 | (096) 502 30 **
 Volkswagen Jetta 2015                                 | Volkswagen | Jetta                  | 11990 | (050) 076 00 **
 BMW 750 L diesel x-drive  2017                        | BMW        | 750                    | 74999 | (067) 329 55 **
 Mitsubishi L 200 2017                                 | Mitsubishi | L 200                  | 29000 | (095) 650 60 **
 Toyota RAV4 2016                                      | Toyota     | RAV4                   | 31500 | (096) 995 57 **
 Audi A8 3.0 TDI QUATTRO LONG 2017                     | Audi       | A8                     | 49999 | (050) 317 11 **
```
