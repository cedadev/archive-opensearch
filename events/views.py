from rest_framework import generics
from rest_framework import mixins
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.views import APIView
from rest_framework.viewsets import GenericViewSet

from .models import Event
from .serialisers import EventSerializer


class EventViewSet(
    GenericViewSet,
    generics.ListAPIView,
    mixins.ListModelMixin,
    mixins.CreateModelMixin,
    generics.GenericAPIView,
    APIView,
):
    """Get a list of Event Objects.

    Available end points:
        - /api/events/ - Will list all Events in the database
        - /api/events/?<Query>=<Value>/ - returns list of all Events matching the query

    Available Methods:
        - GET
        - POST

    Available filters:
        - from_date
        - collection_id
        - action

    How to use filters:
        These filters can be used like django query filters.
        - /api/events/?from_date=2021-08-05
        - /api/events/?action=updated
        - /api/events/?collection_id=ef1627f523764eae8bbb6b81bf1f7a0a

        Filters can be stack to be an 'AND' relationship. e.g.
        - /api/events/?from_date=2021-08-05&action=updated
    """

    queryset = Event.objects.all()
    serializer_class = EventSerializer
    permission_classes = [IsAuthenticatedOrReadOnly]

    def get_serializer(self, *args, **kwargs):
        """Override get_serializer: adds many=True to kwargs if post input is list else call default method"""
        if "data" in kwargs:
            data = kwargs["data"]
            if isinstance(data, list):
                kwargs["many"] = True
        return super(EventViewSet, self).get_serializer(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        """Get request method returns list of all Event model objects"""
        return self.list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """Post request method returns a call to create Event model via serializer"""
        return self.create(request, *args, **kwargs)

    def get_queryset(self):
        """Get request method with queryset, returns filtered list of Event model objects

        Usage: url/?<query>=<value>&...
        Where query is choice from:
        -   'from_date': filter events from this date in YYYY-MM-DD format
        -   'collection_id': filter events of a particular collection id
        -   'action': filter event by action of choice ['added','removed','updated']
        """
        queryset = Event.objects.all()
        datetime = self.request.query_params.get("from_date")
        collection_id = self.request.query_params.get("collection_id")
        action = self.request.query_params.get("action")

        if datetime:
            queryset = queryset.filter(datetime__gte=datetime)
        if collection_id:
            queryset = queryset.filter(collection_id=collection_id)
        if action:
            queryset = queryset.filter(action=action)

        return queryset
