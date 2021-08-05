from rest_framework import mixins, generics
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.views import APIView
from rest_framework.viewsets import GenericViewSet

from .models import Event
from .serialisers import EventSerializer


class EventViewSet(GenericViewSet,
                   generics.ListAPIView,
                   mixins.ListModelMixin,
                   mixins.CreateModelMixin,
                   generics.GenericAPIView,
                   APIView,):
    queryset = Event.objects.all()
    serializer_class = EventSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]

    def get_serializer(self, *args, **kwargs):
        """
        Override get_serializer: call serializer and change many parameter to True if post request is a list
        else use default super get_serializer
        """
        if "data" in kwargs:
            data = kwargs["data"]
            if isinstance(data, list):
                kwargs["many"] = True
        return super(EventViewSet, self).get_serializer(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.create(request, *args, **kwargs)

    def get_queryset(self):
        # get request filters against query parameters from url
        queryset = Event.objects.all()
        datetime = self.request.query_params.get('from_date')
        collection_id = self.request.query_params.get('collection_id')
        action = self.request.query_params.get('action')

        if datetime:
            queryset = queryset.filter(datetime__gte=datetime)
        if collection_id:
            queryset = queryset.filter(collection_id=collection_id)
        if action:
            queryset = queryset.filter(action=action)

        return queryset
