# opensearch_service/api.py
from flask import Flask, request, jsonify
from opensearchpy import OpenSearch
import json
import config

app = Flask(__name__)

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
    http_auth=(config.OPENSEARCH_USERNAME, config.OPENSEARCH_PASSWORD)
)

@app.route('/create_index', methods=['POST'])
def create_index():
    with open('schema.json') as file:
        schema = json.load(file)
    client.indices.create(index='news_articles', body=schema, ignore=400)
    return jsonify({"message": "Index created"}), 200

@app.route('/index_article', methods=['POST'])
def index_article():
    article = request.json

    # Transform esg_tagger into a list of objects
    esg_tagger_items = [{"tag": key, "score": value} for key, value in article.get("esg_tagger", {}).items()]
    article["esg_tagger"] = esg_tagger_items

    response = client.index(index='news_articles', document=article)
    return jsonify(response), 200

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query')
    esg_tag = request.args.get('esg_tag')  # Optional: for exact ESG tag searches

    search_body = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        }
    }

    # Add text search if query is provided
    if query:
        search_body["query"]["bool"]["should"].extend([
            {"match": {"title": query}},
            {"match": {"body": query}}
        ])

    # Add ESG tag search if esg_tag is provided
    if esg_tag:
        search_body["query"]["bool"]["should"].append({
            "nested": {
                "path": "esg_tagger",
                "query": {
                    "term": {"esg_tagger.tag": esg_tag}
                }
            }
        })

    response = client.search(index='news_articles', body=search_body)
    return jsonify(response['hits']['hits']), 200



if __name__ == '__main__':
    app.run(port=5001)  # Run on a different port if your main Flask app is on 5000
