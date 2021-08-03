from django.urls import include, path
from rest_framework import routers
from rest_framework.urlpatterns import format_suffix_patterns
from events import views as eventviews

router = routers.DefaultRouter()
router.register(r'events', eventviews.EventViewSet)

urlpatterns = [
    path('', include(router.urls)),
]