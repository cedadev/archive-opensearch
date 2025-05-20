from django.db import models


class Event(models.Model):
    ACTION_CHOICES = [
        ("added", "added"),
        ("updated", "updated"),
        ("removed", "removed"),
    ]
    # max_length should be 32 but difftest index has a 35 char collection_id
    # because of added 'aaa' on the end.
    collection_id = models.CharField(max_length=50)
    collection_title = models.CharField(max_length=1024)
    action = models.CharField(max_length=8, choices=ACTION_CHOICES)
    datetime = models.DateField()

    def __str__(self):
        return f"{self.collection_title} ({self.action})"
