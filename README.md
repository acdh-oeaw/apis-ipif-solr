APIS IPIF (Solr implementation)
===============================

Repository for implementation of [IPIF Prosopography API](https://github.com/GVogeler/prosopogrAPhI) for APIS, using Solr indexes for data storage and querying.

## Installation

```python
# Your settings file:

APIS_IPIF_CONFIG = {
    "URL": "http://localhost:8983/solr/test_solr", # The address of Solr instance
    "MAX_CHUNK_SIZE": 5000 # Max number of documents to push to Solr at a time
}

INSTALLED_APPS = [
    ...
    "apis_ipif_solr",
    ...
]
```

```python
# urls.py (maybe already added to apis-core):

if "apis_ipif_solr" in settings.INSTALLED_APPS:
    urlpatterns.append(url(r"^ipif/", include("apis_ipif_solr.urls", namespace="ipif")))
```

To create an IPIF Solr index, run `python manage.py runscript build_indexes`.

IPIF endpoint is served from `<APIS-INSTANCE>/ipif/`.