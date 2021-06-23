from elasticsearch import Elasticsearch
import argparse


def pars_arg():
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--port", type=str, default="9200")
    ap.add_argument("-o", "--host", type=str, default="elasticsearch")
    ap.add_argument("-i", "--index", action='store_true')
    ap.add_argument("-s", "--surname")
    ap.add_argument("-u", "--subject")
    ap.add_argument("-m", "--mark")
    return vars(ap.parse_args())


def conn_es(host, port):
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Подключилсь')
    else:
        print('Не подключились')
    return es


def create_index(es_object):
    created = False
    settings = {
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
                    "name": {
                        "type": "keyword"
                    },
                    "subject": {
                        "type": "keyword"
                    },
                    "mark": {
                        "type": "integer"
                    },
                    "date_mark": {
                        "type": "date",
                        "format": "yyyy-dd-MM"
                    }
                }
            }
        }
    }

    try:
        if not es_object.indices.exists('marks_student'):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index='marks_student', ignore=400, body=settings)
            print('Создали индекс')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


if __name__ == '__main__':
    args = pars_arg()
    print(args)
    elastic = conn_es(args['host'], args['port'])

    if args['index']:
        create_index(elastic)
        exit(0)

    info = {'surname': args['surname'], 'subject': args['subject'], 'mark': args['mark']}
    elastic.index(index='marks_student', doc_type='students_mark_info', body=info)
    print("Добавили")




PUT /secret_db
  {
  "settings": {
    "analysis": {
      "filter": {
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_keywords": {
          "type":       "stop",
          "stopwords": ["рома","иван","кирилл"]
        }
      },
      "char_filter": {
        "grifs": {
          "type": "mapping",
          "mappings": [
            "os_vaj => не интересссно",
            "sovsec => не интерессно",
            "sec => не интересно"
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
     "mappings":{
       "_doc":{
         "properties":{
            "secure": {
               "type":"text",
               "analyzer":"griff_changer",
                "search_analyzer": "standard"
           },
             "attachment":{
               "properties":{
                 "content":{
                   "type":"text",
                 "analyzer":"griff_changer",
                  "search_analyzer": "standard"
                 }
               }
             }
          }
        }
     }
}