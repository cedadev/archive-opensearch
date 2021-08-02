from django.db import models


class Event(models.Model):
    EVENT_ACTION_CHOICES = ['added', 'removed']

    collection_id = models.CharField(max_length=50)
    collection_title = models.CharField(max_length=1024)
    action = models.CharField(max_length=8, choices=EVENT_ACTION_CHOICES)
    datetime = models.DateField()
