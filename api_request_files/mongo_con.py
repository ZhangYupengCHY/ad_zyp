# author: marmot

import traceback

import pymongo


# Mongodb
class MongoDb:
    """
    Mongodb数据库管理
    """

    def __init__(self, db_name, db_sheet="", username="root", password="415263"):
        self.connect = f"mongodb://{username}:{password}@127.0.0.1:27017"
        # self.connect = "mongodb://127.0.0.1:27017/config"
        self.db_name = db_name
        self.db_sheet = db_sheet

    @property
    def mongo_db_list(self):
        con_client = pymongo.MongoClient(self.connect)
        db_list = con_client.list_database_names()
        con_client.close()

        return db_list

    @property
    def mongo_sheets_list(self):
        con_client = pymongo.MongoClient(self.connect)
        con_db = con_client[self.db_name]
        con_sheets = con_db.list_collection_names()
        con_client.close()

        return con_sheets

    def mongo_insert(self, insert_list):
        """
        :param insert_list: [{}]: 如[{"name": "Taobao"},{"name": "QQ"}]
        :return:
        """
        con_client = pymongo.MongoClient(self.connect)
        con_db = con_client[self.db_name]
        con_sheet = con_db[self.db_sheet]
        id_log = con_sheet.insert_many(insert_list)

        con_client.close()

        return id_log.inserted_ids

    def mongo_update(self, find_focus, new_values):
        """
        :param find_focus: {}：如{"name": {"$regex": "^F"}}或{"name": 'Facebook'}
        :param new_values: {}：如{"alexa": "123"}
        :return:
        """
        con_client = pymongo.MongoClient(self.connect)
        con_db = con_client[self.db_name]
        con_sheet = con_db[self.db_sheet]
        update_items = con_sheet.update_many(find_focus, {"$set": new_values})

        update_items = update_items.modified_count

        con_client.close()

        return update_items

    def mongo_del(self, find_focus):
        """
        :param find_focus: {}:如{"name": "marmot"}
        :return:
        """
        con_client = pymongo.MongoClient(self.connect)
        con_db = con_client[self.db_name]
        con_sheet = con_db[self.db_sheet]
        con_client.close()
        del_items = con_sheet.delete_many(find_focus)

        del_num = del_items.deleted_count

        return del_num

    def mongo_select(self, find_focus=None, if_limit=None, sort_key=None):
        """
        :param find_focus: None, ({}): 如[{"_id": 0, "name": 1, "alexa": 1}], {}:如{"name": "marmot"}, [{},{}]
        :param if_limit:
        :param sort_key: []: 如["alexa", 1]
        :return:
        """
        if sort_key is None:
            sort_key = []
        con_client = pymongo.MongoClient(self.connect)
        con_db = con_client[self.db_name]
        con_sheet = con_db[self.db_sheet]
        if find_focus is None:
            total_items = con_sheet.find()
        elif isinstance(find_focus, tuple):
            total_items = con_sheet.find({}, find_focus[0])
        elif isinstance(find_focus, list):
            total_items = con_sheet.find({'$or': find_focus}, {'_id': 0})
        elif isinstance(find_focus, dict):
            total_items = con_sheet.find(find_focus, {'_id': 0})
        else:
            raise traceback.format_exc()
        con_client.close()

        if if_limit is None:
            result = total_items
        else:
            result = total_items.limit(if_limit)
        if sort_key is not None:
            if sort_key:
                result = result.sort(sort_key[0:-1], sort_key[-1])

        return [one_res for one_res in result]

    def mongo_drop(self):
        """
        :return:
        """
        con_client = pymongo.MongoClient(self.connect)
        con_db = con_client[self.db_name]
        con_sheet = con_db[self.db_sheet]
        result = con_sheet.drop()
        con_client.close()

        return result


if __name__ == "__main__":
    admin_mongo = MongoDb('admin')
    print(admin_mongo.mongo_sheets_list)
    test_mongo = MongoDb('runoobdb', 'sites')
    # # # print(test_mongo.mongo_del({'name': 'Facebook'}))
    mylist = [
        {"name": "Taobao", "alexa": "100", "url": "https://www.taobao.com"},
        {"name": "QQ", "alexa": "101", "url": "https://www.qq.com"},
        {"name": "Facebook", "alexa": "10", "url": "https://www.facebook.com"},
        {"name": "知乎", "alexa": "103", "url": "https://www.zhihu.com"},
        {"name": "Github", "alexa": "109", "url": "https://www.github.com"}
    ]
    # test_mongo.mongo_insert([{"name": "Taobao", "alexa": "100", "url": "https://www.taobao.com"}])
    test_mongo.mongo_insert(mylist)
    print(test_mongo.mongo_sheets_list)
    print(test_mongo.mongo_select([{"name": "Taobao", "alexa": "100"},
                                   {"name": "QQ", "alexa": "101"}]))
    print(test_mongo.mongo_del({}))

    print(test_mongo.mongo_select())
    # # print(test_mongo.mongo_update({'name': 'Facebook'}, {"alexa": "20", "url": "", 'real_name': ''}))
    # # print(test_mongo.mongo_select({'name': 'Facebook'}))
