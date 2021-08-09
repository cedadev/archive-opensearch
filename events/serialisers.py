from rest_framework import fields
from rest_framework import serializers

from .models import Event


class EventSerializer(serializers.HyperlinkedModelSerializer):
    """Serializer class to transform JSON or list of JSON to django model.

    Expected format produced from script:
    ```
    {
        "collection_id": String, max_length=50
        "collection_title": String, max_length=1024
        "action": Choice of ['added', 'updated', 'removed']
        "datetime": ISO-8601 datetime format
    }
    ```
    """

    datetime = fields.DateField(input_formats=["%Y-%m-%dT%H:%M:%S.%f"])

    class Meta:
        model = Event
        fields = [
            "collection_id",
            "collection_title",
            "action",
            "datetime",
        ]
