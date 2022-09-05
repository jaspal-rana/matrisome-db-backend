from tinydb import TinyDB, Query
import re


class BlogRepo:
    def __init__(self):
        self.db_name = 'blog_db.json'
        self.db = TinyDB(self.db_name)

    def insert(self, collection_name, data):
        data['collection'] = collection_name
        self.db.insert(data)
        del data['collection']

    def find_all(self, collection_name):
        CollectionQuery = Query()
        docs = self.db.search(CollectionQuery['collection'] == collection_name)
        for doc in docs:
            doc['id'] = doc.doc_id

        return docs

    def delete_by_id(self, id):
        self.db.remove(doc_ids=[id])

    def delete_all(self):
        self.db.truncate()

    def search(self, search_filters):
        SearchQuery = Query()
        exactMatchVar = None
        if 'exact_match' in search_filters:
            for k, v in search_filters['exact_match'].items():
                if exactMatchVar is None:
                    exactMatchVar = SearchQuery[k] == v
                else:
                    exactMatchVar = exactMatchVar & (SearchQuery[k] == v)

        partialMatchVar = None
        if 'partial_match' in search_filters:
            for field in search_filters['partial_match']['fields']:
                if partialMatchVar is None:
                    partialMatchVar = SearchQuery[field].matches(search_filters['partial_match']['search_term'], re.IGNORECASE)
                else:
                    partialMatchVar = partialMatchVar | SearchQuery[field].matches(search_filters['partial_match']['search_term'], re.IGNORECASE)

        matchVar = None
        if exactMatchVar and partialMatchVar:
            matchVar = exactMatchVar & partialMatchVar
        elif exactMatchVar:
            matchVar = exactMatchVar
        elif partialMatchVar:
            matchVar = partialMatchVar

        if matchVar:
            docs = self.db.search(matchVar)
        else:
            docs = self.db.all()
        for doc in docs:
            doc['id'] = doc.doc_id

        return docs


if __name__ == '__main__':
    blog_repo = BlogRepo('blog_db.json')
    data = {
        "blog_name": 'hello',
        "blog_header": {
            "type": "text",
            "content": "This is a blog header",
            "color": "#ffffff",
            "font": "Bold"
        },
        "blog_content": [
            {
                "type": "text",
                "content": "This is a blog header",
                "color": "#ffffff",
                "font": "Bold"
            }
        ]
    }
    # blog_repo.insert('test', data)
    blog_repo.delete_by_id(5)
    print(blog_repo.find_all(('test')))

    