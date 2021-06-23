from elasticsearch import Elasticsearch
import argparse
import os
import statistics
import sys


def pars_arg():
    ap = argparse.ArgumentParser()
    ap.add_argument("command")
    ap.add_argument("command_second", nargs='?', default=None)
    ap.add_argument("-p", "--port", type=int, default=9200)
    ap.add_argument("-o", "--host", type=str, default='elasticsearch')
    ap.add_argument("-a", "--author")
    ap.add_argument("-y", "--year")
    ap.add_argument("-n", "--name")
    ap.add_argument("-f", "--file")
    ap.add_argument("-r", "--from")
    ap.add_argument("-t", "--till")
    ap.add_argument("-w", "--word")

    return ap.parse_args()


def conn_es(host, port):
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Подключилсь')
    else:
        print('Не подключились')
    return es


def create_index(es_object, index_name):
    created = False
    body_books = {
        "settings": {
            "analysis": {
                "filter": {
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_keywords": {
                        "type": "stop",
                        "stopwords": ["князь", "повезет", "сорок"]
                    }
                },

                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
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
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "search_analyzer": "standard"
                    },
                    "author": {
                        "type": "text",
                        "analyzer": "standard",
                        "search_analyzer": "standard"
                    },
                    "year_publication": {
                        "type": "date",
                        "format": "yyyy"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "custom_analyzer",
                        "search_analyzer": "custom_analyzer"
                    }
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=body_books)
            print(f'Создали индекс {index_name}')
            created = True
        else:
            print(f"Индекс {index_name} уже создан")
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    return res


def termsvector(es_object, index_name, id, body):
    res = es_object.termvectors(index=index_name, doc_type="document", id=id, body=body)
    return res


def search_count_books_with_words(elastic, index_name, word):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"text": f"{word}"}
                    }
                ]
            }
        }
    }

    res = search(elastic, index_name, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, {record['_source']['year_publication']}")


def search_search_books(elastic, index_name, author, word):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"text": f"{word}"}
                    },
                    {
                        "match": {"author": f"{author}"}
                    }
                ]
            }
        }
    }
    res = search(elastic, index_name, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and author")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, {record['_source']['year_publication']}")


def search_date(elastic, index_name, from_date, till_date, word):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "year_publication": {
                                "gte": from_date,
                                "lte": till_date
                            }
                        }
                    }

                ],
                "must_not": {
                    "match": {"text": f"{word}"}
                }
            }
        }
    }
    res = search(elastic, index_name, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and date range")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, {record['_source']['year_publication']}")


def calc_date(elastic, index_name, author):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"author": f"{author}"}
                    }
                ]
            }
        }
    }
    res = search(elastic, index_name, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this author")
        exit(0)
    years = []
    for record in res['hits']['hits']:
        years.append(int(record['_source']['year_publication']))
    print(statistics.mean(years))


def search_by_year(elastic, index_name, year):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"year_publication": year}
                    }
                ]
            }
        }
    }
    res = search(elastic, index_name, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this year")
        exit(0)
    ids = []
    for record in res['hits']['hits']:
        ids.append(record['_id'])
    print(ids)
    return ids


def top_words(elastic, index_name, year):
    terms = {}
    body = {

        "fields": ["text"],
        "offsets": False,
        "payloads": False,
        "positions": False,
        "term_statistics": False,
        "field_statistics": False
    }

    ids = search_by_year(elastic, index_name, year)

    for id in ids:
        res = termsvector(elastic, index_name, id, body)
        for term in res['term_vectors']['text']['terms'].keys():
            if term in terms.keys():
                terms[term] += res['term_vectors']['text']['terms'][term]['term_freq']
            else:
                terms[term] = res['term_vectors']['text']['terms'][term]['term_freq']

    # print(terms)

    sorted_tuples = sorted(terms.items(), key=lambda item: item[1], reverse=True)  #сортировка сллваря по значению
    sorted_dict = {k: v for k, v in sorted_tuples}
    print("Частые слова")
    # print(sorted_dict)
    for fre in list(sorted_dict.keys())[1:100]:
        print(fre, " : ", sorted_dict[fre])


if __name__ == '__main__':
    args = pars_arg()
    print(args)
    elastic = conn_es(args.host, args.port)
    index_name = '2017-3-26-ush'

    if args.command == 'create':
        create_index(elastic, index_name)
        exit(0)
    elif args.command == 'add-book':
        if args.command_second and args.command_second == 'books':
            files = os.listdir(f'document/{args.command_second}')
            for file in files:
                name_file = file
                file = file.split('.')[0]
                file_shard = file.split('-')
                if len(file_shard) == 3:
                    name = file_shard[0].replace(' ', '')
                    author = file_shard[1]
                    year = file_shard[2].replace(' ', '')
                    with open(f"document/{args.command_second}/{name_file}", 'r', encoding='utf-8') as f:
                        elastic.index(index=index_name, doc_type='document', body={
                            'title': name,
                            'author': author,
                            'year_publication': year,
                            'text': f.read()
                        })
        else:
            with open(f"document/{args.file}", 'r', encoding='utf-8') as f:
                elastic.index(index=index_name, doc_type='document', body={
                    'title': args.name,
                    'author': args.author,
                    'year_publication': args.year,
                    'text': f.read()
                })
    elif args.command == 'count-books-with-words':
        if args.command_second:
            search_count_books_with_words(elastic, index_name, args.command_second)
        else:
            print("No word")
            exit(1)
    elif args.command == 'search-books':
        if args.command_second and args.author:
            search_search_books(elastic, index_name, args.author, args.command_second)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'search-dates':
        if args.from_date and args.till_date and args.word:
            search_date(elastic, index_name, args.from_date, args.till_date, args.word)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'calc-date':
        if args.author:
            calc_date(elastic, index_name, args.author)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'top-words':
        if args.year:
            top_words(elastic, index_name, args.year)
        else:
            print("Error args")
            exit(1)
    else:
        print("Unknown command")
        exit(1)
