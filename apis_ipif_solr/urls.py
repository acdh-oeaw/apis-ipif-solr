from django.urls import path

from django.conf.urls.static import static
from .settings import STATIC_URL
from .api_views import PersonListView

app_name = "apis_ipif_solr"

urlpatterns = [path("person/", PersonListView.as_view()),] + static(STATIC_URL)
