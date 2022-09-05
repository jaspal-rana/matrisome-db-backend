import json
from flask import Flask, jsonify, request
from blog_repo import BlogRepo
import os
import pandas as pd

app = Flask(__name__)
blog_repo = BlogRepo()


@app.route("/blog/fetch-all")
def fetch_all():
    blog_type = request.args.get('blog-type')
    rsp = jsonify(blog_repo.find_all(blog_type))
    rsp.headers['Access-Control-Allow-Origin'] = '*'

    return rsp


@app.route("/blog/add", methods=['POST'])
def add_blog():
    blog_type = request.args.get('blog-type')
    blog_data = json.loads(request.data)
    blog_repo.insert(blog_type, blog_data)

    rsp = jsonify({
        'status': 'OK'
    })
    rsp.headers['Access-Control-Allow-Origin'] = '*'

    return rsp


@app.route("/blog/delete")
def delete_blog_by_id():
    id = int(request.args.get('id'))
    blog_repo.delete_by_id(id)

    rsp = jsonify({
        'status': 'OK'
    })
    rsp.headers['Access-Control-Allow-Origin'] = '*'

    return rsp

@app.route("/blog/delete-all")
def delete_all():
    blog_repo.delete_all()

    rsp = jsonify({
        'status': 'OK'
    })
    rsp.headers['Access-Control-Allow-Origin'] = '*'

    return rsp

@app.route("/blog/initialise")
def initialise():
    data_limit = request.args.get('data-limit', default=1000)
    blog_repo.delete_all()

    files = []
    for (dirpath, dirnames, filenames) in os.walk('./data'):
        files.extend(filenames)
    json_files = [f for f in files if f[-5:] == '.json']
    for f in json_files:
        with open('./data/' + f, encoding='utf-8') as fp:
            blog_type = f[:-5]
            blog_data = json.load(fp)

            if blog_type == 'search_data':
                for part in blog_data:
                    blog_repo.insert(blog_type, part)
            else:
                blog_repo.insert(blog_type, blog_data)

    csv_files = [f for f in files if f[-4:] == '.csv']
    for f in csv_files:
        blog_type = f[:-4]
        df = pd.read_csv('./data/' + f, header=[0])
        df = df.dropna()
        df = df.sample(data_limit)
        columns = [c for c in df.columns]
        json_data_list = []
        for row in df.values:
            json_data = {}
            for i in range(0, len(row)):
                json_data[columns[i]] = row[i]
            json_data_list.append(json_data)

        print([e for e in json_data_list if e['Gene'] == "COL1A1"])

        for i, part in enumerate(json_data_list):
            if i % 50 == 0:
                print('data import progress : {}/{}'.format(i, min(data_limit, len(json_data_list))))
            blog_repo.insert(blog_type, part)

    rsp = jsonify({
        'status': 'OK'
    })
    rsp.headers['Access-Control-Allow-Origin'] = '*'

    return rsp

@app.route("/blog/search", methods=['POST', 'OPTIONS'])
def search_blog():
    if request.method == 'OPTIONS':
        rsp = jsonify({
            'status': 'OK'
        })
        rsp.headers['Access-Control-Allow-Origin'] = '*'
        rsp.headers['Access-Control-Allow-Headers'] = '*'

        return rsp
    
    search_filters = json.loads(request.data)
    rsp = blog_repo.search(search_filters)

    rsp = jsonify(rsp)
    rsp.headers['Access-Control-Allow-Origin'] = '*'

    return rsp


if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)
