from django.urls import path, re_path

from .api_views import (
    FactoidsListView,
    FactoidsView,
    PersonsListView,
    PersonsView,
    SourcesListView,
    StatementsListView,
    StatementsView,
)

app_name = "apis_ipif_solr"

urlpatterns = [
    path("persons/", PersonsListView.as_view()),
    re_path(r"persons/(?P<id>(https?://)?.*)$", PersonsView.as_view()),
    path("factoids/", FactoidsListView.as_view()),
    re_path(r"factoids/(?P<id>(https?://)?.*)$", FactoidsView.as_view()),
    path("statements/", StatementsListView.as_view()),
    re_path(r"statements/(?P<id>(https?://)?.*)$", StatementsView.as_view()),
    path("sources/", SourcesListView.as_view()),
]
