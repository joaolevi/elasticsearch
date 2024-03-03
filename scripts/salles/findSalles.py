from elasticsearch import Elasticsearch

# Conectar ao Elasticsearch
es = Elasticsearch(["http://127.0.0.1:9200"])

def find_salles(valor: str):
    resp = es.search(index="salles", body={"query": {"match_phrase": {"review.review_comment_message" : valor}}})

    # Imprimir o número de hits encontrados
    print("Encontrado %d Hits:" % resp['hits']['total']['value'])

    # Iterar sobre os hits retornados pela consulta
    for hit in resp['hits']['hits']:
        # Verificar se os campos 'review_comment_message' e 'product_category_name' existem no documento
        if 'review' in hit['_source'] and 'review_comment_message' in hit['_source']['review'] \
            and 'items' in hit['_source'] and 'product_category_name' in hit['_source']['items'][0]:
            # Se os campos existirem, extrair e imprimir seus valores
            review_message = hit['_source']['review']['review_comment_message']
            category_name = hit['_source']['items'][0]['product_category_name']
            score = hit['_score']
            print(f"Score: {score}; Review Comment: {review_message}; Product Category: {category_name}")
        else:
            # Se algum dos campos estiver faltando, imprimir uma mensagem de aviso
            print("Algum dos campos necessários está faltando neste documento.")

if __name__ == "__main__":
    valor = ""
    while valor != "sair":
        valor = input("\nDigite o valor a ser pesquisado. Para sair digita 'sair': ")
        find_salles(valor)
