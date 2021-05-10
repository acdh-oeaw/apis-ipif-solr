APIS IPIF (Solr implementation)
===============================

Repository for implementation of IPIF API for APIS, using Solr indexes for data storage and querying.

## Installation

```python
# Your settings file:

APIS_IPIF_CONFIG = {"URL": <THE URL OF YOUR SOLR INSTANCE>}

INSTALLED_APPS = [
    ...
    "apis_ipif_solr",
    ...
]
```