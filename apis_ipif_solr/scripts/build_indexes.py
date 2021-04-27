from apis_ipif_solr.indexes import PySolaar, PersonIndex


def run():
    print("running!")
    PySolaar.update(max_chunk_size=5000)

