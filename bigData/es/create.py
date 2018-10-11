from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class Elasticobj:
#   self    代表的是类的实例，代表当前对象的地址，而
#    self.class 则指向类。

# __init__  为构造方法
# 属性前面有 __ 的表示为私有属性，私有属性在类外部无法直接进行访问，只能在类的内部调用
# 实例不能访问私有变量
    def __init__(self,ip="localhost",port="9200"):
        '''
        :param ip:
        :param port:
        '''
        self.ip = ip
        self.port = port
        self.es = Elasticsearch([ip+":"+port])

# body = {
#     "name":"elena",
#     "mobile":"18475637852",
#     "age":18
# }
#
# obj = Elasticobj()
# obj.es.index(index = "index",doc_type = 'contact',body=body)
# 没有测成功
# 类方法必须包含参数 self ，且为第一个参数，self 代表的是类的实例
    def create_index(self,index_name,index_type):
        '''
        :param index_name:
        :param index_type:
        :return:
        '''
        _index_mappings ={
            "mappings":{
            index_type:{
                "properties": {
                    "title": {
                        "type": "text",
                        "index": True,
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_max_word"
                    },
                    "date": {
                        "type": "text",
                        "index": True
                    },
                    "keyword": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "source": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "link": {
                        "type": "string",
                        "index": "not_analyzed"
                    }
                }
            }
        }
    }
        if self.es.indices.exists(index=index_name) is not True:
            self.es.indices.create(index=index_name,body=_index_mappings,ignore=400)
            self.es.indices.put_mapping(index=index_name, doc_type=index_type, body=_index_mappings)
            # print(res)

    def Index_Data(self,index_name,index_type):
        '''
        数据存储到es
        :return:
        '''
        list = [
            {"date": "2017-09-13",
             "source": "慧聪网",
             "link": "http://info.broadcast.hc360.com/2017/09/130859749974.shtml",
             "keyword": "电视",
             "title": "付费 电视 行业面临的转型和挑战"
             },
            {"date": "2017-09-13",
             "source": "中国文明网",
             "link": "http://www.wenming.cn/xj_pd/yw/201709/t20170913_4421323.shtml",
             "keyword": "电视",
             "title": "电视 专题片《巡视利剑》广获好评：铁腕反腐凝聚党心民心"
             }
        ]
        for item in list:
            res = self.es.index(index=index_name, doc_type=index_type, body=item)
            print(res["_index"])

    def bulk_Index_Data(self,index_name,index_type):
        '''
        用bulk将批量数据存储到es
        :return:
        '''
        list = [
            {"date": "2017-09-13",
             "source": "慧聪网",
             "link": "http://info.broadcast.hc360.com/2017/09/130859749974.shtml",
             "keyword": "电视",
             "title": "付费 电视 行业面临的转型和挑战"
             },
            {"date": "2017-09-13",
             "source": "中国文明网",
             "link": "http://www.wenming.cn/xj_pd/yw/201709/t20170913_4421323.shtml",
             "keyword": "电视",
             "title": "电视 专题片《巡视利剑》广获好评：铁腕反腐凝聚党心民心"
             },
            {"date": "2017-09-13",
             "source": "人民电视",
             "link": "http://tv.people.com.cn/BIG5/n1/2017/0913/c67816-29533981.html",
             "keyword": "电视",
             "title": "中国第21批赴刚果（金）维和部隊启程--人民 电视 --人民网"
             },
            {"date": "2017-09-13",
             "source": "站长之家",
             "link": "http://www.chinaz.com/news/2017/0913/804263.shtml",
             "keyword": "电视",
             "title": "电视 盒子 哪个牌子好？ 吐血奉献三大选购秘笈"
             }
        ]
        ACTIONS = []
        i = 1
        for line in list:
            action = {
                "_index": index_name,
                "_type": index_type,
                "_id": i,  # _id 也可以默认生成，不赋值
                "_source": {
                    "date": line['date'],
                    "source": line['source'],
                    "link": line['link'],
                    "keyword": line['keyword'],
                    "title": line['title']}
            }
            i += 1
            ACTIONS.append(action)
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=index_name, raise_on_error=True)
        print('Performed %d actions' % success)


obj = Elasticobj()
obj.create_index("app","context")
# obj.Index_Data("news","context")
#obj.bulk_Index_Data("news","context")