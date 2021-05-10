from django.conf import settings


from apis_ipif_solr.indexes import PySolaar


def run():
    print("--------------------------")
    chunk_size = settings.APIS_IPIF_CONFIG.get("MAX_CHUNK_SIZE", 5000)
    print("Building indexes on server:")
    print(settings.APIS_IPIF_CONFIG.get("URL"))
    print("with max chunk size:", chunk_size)
    print("--------------------------")
    PySolaar.update(max_chunk_size=chunk_size)

