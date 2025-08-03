from elasticsearch import Elasticsearch

def list_indices(host='10.20.2.21', port=59200, scheme='http'):
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': scheme}])

    try:
        indices = es.cat.indices(format='json')
        index_names = sorted([idx['index'] for idx in indices])
        print("📦 Elasticsearch Index List:")
        for name in index_names:
            print(f" - {name}")
    except Exception as e:
        print("❌ Failed to fetch index list:", e)

if __name__ == "__main__":
    list_indices()
