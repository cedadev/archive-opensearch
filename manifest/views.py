from django.shortcuts import render
from django.http import JsonResponse, Http404
from elasticsearch import Elasticsearch
from django.conf import settings
from elasticsearch.exceptions import NotFoundError
import json


def get_manifest(request, uuid):

    es = Elasticsearch(
        hosts=settings.ELASTICSEARCH_HOSTS,
        headers={'x-api-key':settings.ES_API_KEY}
    )

    try:
        response = es.get(index=settings.ELASTICSEARCH_COLLECTION_INDEX, id=uuid, _source_includes=['manifest'])
        response = json.loads(response['_source']['manifest'])
        return JsonResponse(response)

    except (NotFoundError, KeyError):
        raise Http404(f'Manifest not found for collection {uuid}')


