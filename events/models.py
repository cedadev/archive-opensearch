from django.db import models


class Event(models.Model):

    def __str__(self):
        return f'{self.collection_title} ({self.action})'

    collection_id = models.CharField(max_length=50)
    collection_title = models.CharField(max_length=1024)
    action = models.CharField(max_length=8)
    datetime = models.DateField()
