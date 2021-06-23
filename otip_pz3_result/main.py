from elasticsearch import Elasticsearch
import argparse
import random


def pars_arg():
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--port", type=str, default='9200')
    ap.add_argument("-o", "--host", type=str, default='elasticsearch')
    ap.add_argument("-i", "--index_creat")
    ap.add_argument("-s", "--surname")
    ap.add_argument("-u", "--subject")
    ap.add_argument("-m", "--mark")
    ap.add_argument("-d", "--date")
    ap.add_argument("-f", "--file")
    ap.add_argument("-g", "--griph")
    ap.add_argument("-sl", "--search_lucki", action='store_true')
    ap.add_argument("-sdr", "--search_date", action='store_true')
    ap.add_argument("-y", "--year")
    ap.add_argument("-l", "--mount")

    return vars(ap.parse_args())


def conn_es(host, port):
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Подключилсь')
    else:
        print('Не подключились')
    return es


def create_index(es_object, index):
    created = False
    body_griph = {
        "settings": {
            "analysis": {
                "filter": {
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_keywords": {
                        "type": "stop",
                        "stopwords": ["андрей", "алексей", "никита"]
                    }
                },
                "char_filter": {
                    "grifs": {
                        "type": "mapping",
                        "mappings": [
                            "не интересно => особой важности",
                            "не интересcно => совершенно секретно",
                            "не интереccсно => секретно",
                            "не интереcсcсно => для служебного пользования",
                        ]
                    }
                },
                "analyzer": {
                    "griff_changer": {
                        "tokenizer": "standard",
                        "char_filter": [
                            "grifs"
                        ],
                        "filter": [
                            "lowercase",
                            "russian_stop",
                            "russian_keywords"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "document": {
                "properties": {
                    "name": {
                        "type": "text",
                        "analyzer": "griff_changer",
                        "search_analyzer": "standard"
                    },
                    "griph": {
                        "type": "text",
                        "analyzer": "griff_changer",
                        "search_analyzer": "standard"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "griff_changer",
                        "search_analyzer": "standard"
                    }
                }
            }
        }
    }
    body_mark = {
        "settings": {
            "analysis": {
                "analyzer": {
                }
            }
        },
        "mappings": {
            "students_mark_info": {
                "properties": {
                    "surname": {
                        "type": "keyword"
                    },
                    "subject": {
                        "type": "keyword"
                    },
                    "mark": {
                        "type": "integer"
                    },
                    "luck": {
                        "type": "keyword"
                    },
                    "date_mark": {
                        "type": "date",
                        "format": "yyyy-MM-DD"
                    }
                }
            }
        }
    }
    if index == 'mark_student':
        settings = body_mark
    elif index == 'griph_doc':
        settings = body_griph
    else:
        print("no index")
        exit(2)

    try:
        if not es_object.indices.exists(index):
            es_object.indices.create(index=index, ignore=400, body=settings)
            print('Создали индекс')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    return res


def search_date_range(elastic, year, mount):
    body = {
        "query": {
            "range": {
                "date": {
                    "gte": f"{year}-{mount}-01",
                    "lte": f"{year}-{mount}-28"
                }
            }
        }
    }
    res = search(elastic, 'mark_student', body)
    for record in res['hits']['hits']:
        print(record['_source']['mark'])


def search_lucki(elastic, subject, mark):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"subject": f"{subject}"}
                    },
                    {
                        "term": {"mark": mark}
                    },
                    {
                        "term": {"luck": "+"}
                    }
                ]
            }
        }
    }
    res = search(elastic, 'mark_student', body)
    for record in res['hits']['hits']:
        print(record['_source']['surname'])


if __name__ == '__main__':
    args = pars_arg()
    print(args)
    elastic = conn_es(args['host'], args['port'])

    if args['index_creat']:
        create_index(elastic, args['index_creat'])
        exit(0)

    if args['file'] and args['griph']:
        with open(args['file'], 'r', encoding='utf-8') as f:
            elastic.index(index='griph_doc', doc_type='document', body={
                'name': args['file'],
                'griph': args['griph'],
                'text': f.read()
            })
    elif args['surname'] and args['subject'] and args['mark'] and args['date']:
        luck = '-'
        rand = random.randint(1, 100)
        args['mark'] = int(args['mark'])
        if args['mark'] < 5 and rand <= 50:
            luck = '+'
            args['mark'] += 1
            print('lucki')
        elastic.index(index='mark_student', doc_type='students_mark_info', body={
            'surname': args['surname'],
            'subject': args['subject'],
            'mark': args['mark'],
            'luck': luck,
            'date': args['date']
        })
    elif args['search_lucki'] and args['subject'] and args['mark']:
        search_lucki(elastic, args['subject'], args['mark'])
    elif args['search_date'] and args['year'] and args['mount']:
        search_date_range(elastic, args['year'], args['mount'])
    else:
        print("Error args")
