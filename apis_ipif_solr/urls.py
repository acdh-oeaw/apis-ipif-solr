from django.urls import path

from .api_views import PersonListView

app_name = "apis_ipif_solr"

urlpatterns = [
    path("person/", PersonListView.as_view()),
]
