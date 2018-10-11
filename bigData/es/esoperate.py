from elasticsearch.helpers import bulk
import elasticsearch


class ElasticSearchClient():
    @staticmethod
    def get_es_servers():
        es_servers = [{
            "host": "localhost",
            "port": "9200"
        }]
        es_client = elasticsearch.Elasticsearch(hosts=es_servers)
        return es_client


class LoadElasticSearch():
    def __init__(self):
        self.index = "hz"
        self.doc_type = "xyd"
        self.es_client = ElasticSearchClient.get_es_servers()
        # self.set_mapping()

    def set_mapping(self):
        """
        设置mapping
        """
        mapping = {
            self.doc_type: {
                "properties": {
                    "document_id": {
                        "type": "integer"
                    },
                    "title": {
                        "type": "string"
                    },
                    "content": {
                        "type": "string"
                    }
                }
            }
        }

        if not self.es_client.indices.exists(index=self.index):
            # 创建Index和mapping
            self.es_client.indices.create(index=self.index, body=mapping, ignore=400)
            self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)

    def add_date(self, row_obj):
        """
        单条插入ES
        """
        _id = row_obj.get("_id", 1)
        row_obj.pop("_id")
        self.es_client.index(index=self.index, doc_type=self.doc_type, body=row_obj, id=_id)

    def add_date_bulk(self, row_obj_list):
        """
        批量插入ES
        """
        load_data = []
        i = 1
        bulk_num = 2000  # 2000条为一批
        for row_obj in row_obj_list:
            action = {
                "_index": self.index,
                "_type": self.doc_type,
                "_id": row_obj.get('_id', 'None'),
                "_source": {
                    'document_id': row_obj.get('document_id', None),
                    'title': row_obj.get('title', None),
                    'content': row_obj.get('content', None),
                }
            }
            load_data.append(action)
            i += 1
            # 批量处理
            if len(load_data) == bulk_num:
                print('插入', i / bulk_num, '批数据')
                print(len(load_data))
                success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
                del load_data[0:len(load_data)]
                print(success, failed)

        if len(load_data) > 0:
            success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
            del load_data[0:len(load_data)]
            print(success, failed)

    def update_by_id(self, row_obj):
        """
        根据给定的_id,更新ES文档
        :return:
        """
        _id = row_obj.get("_id", 1)
        row_obj.pop("_id")
        self.es_client.update(index=self.index, doc_type=self.doc_type, body={"doc": row_obj}, id=_id)

    def delete_by_id(self, _id):
        """
        根据给定的id,删除文档
        :return:
        """
        self.es_client.delete(index=self.index, doc_type=self.doc_type, id=_id)

if __name__ == '__main__':
    write_obj = {
        "_id": 1,
        "document_id": 1,
        "title": u"Hbase 测试数据",
        "content": u"Hbase 日常运维,这是个假数据监控Hbase运行状况。通常IO增加时io wait也会增加，现在FMS的机器正常情况......",
    }

    load_es = LoadElasticSearch()
    # load_es.set_mapping()
    # 插入单条数据测试
    load_es.add_date(write_obj)

    # 根据id更新测试
    # write_obj["title"] = u"更新标题"
    # load_es.update_by_id(write_obj)

    # 根据id删除测试
    # load_es.delete_by_id(1)

    # 批量插入数据测试
    # row_obj_list = []
    # for i in range(2, 2200):
    #     temp_obj = write_obj.copy()
    #     temp_obj["_id"] = i
    #     temp_obj["document_id"] = i
    #     row_obj_list.append(temp_obj)
    # load_es.add_date_bulk(row_obj_list)