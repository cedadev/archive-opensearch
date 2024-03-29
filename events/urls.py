from django.urls import include, path
from rest_framework import routers
from events import views as eventviews

router = routers.DefaultRouter()
router.register(r'events', eventviews.EventViewSet)

urlpatterns = [
    path('', include(router.urls)),
]
