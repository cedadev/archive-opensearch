from .models import Event
from rest_framework import serializers, fields


class EventSerializer(serializers.HyperlinkedModelSerializer):
    datetime = fields.DateField(input_formats=['%Y-%m-%dT%H:%M:%S.%f'])
    class Meta:
        model = Event
        fields = ['collection_id', 'collection_title', 'action', 'datetime', ]