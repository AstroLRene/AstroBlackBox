from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

list = [
    {
        "name":"astro",
        "mobile":"18900250075",
        "age":18
    },
    {
        "name":"rene",
        "mobile":"13865412589",
        "age":20
    }
]

es = Elasticsearch(['localhost:9200'])
# 单插入
# es.index(index = "index",doc_type = 'contact',body=body)
# 批量插入
ACTIONS = []
i = 1
for line in list:
    action = {
        "_index": "index",
        "_type": "contact",
        "_id": i,  # _id 也可以默认生成，不赋值
        "_source": {
            "name": line['name'],
            "mobile": line['mobile'],
            "age": line['age']}
    }
    i += 1
    ACTIONS.append(action)
    # 批量处理
success, _ = bulk(es, ACTIONS, index="index", raise_on_error=True)
print('Performed %d actions' % success)