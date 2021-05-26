from django.shortcuts import render
from django.http import JsonResponse, Http404
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
from django.conf import settings
from elasticsearch.exceptions import NotFoundError
import json


def get_manifest(request, uuid):

    es = CEDAElasticsearchClient()

    try:
        response = es.get(index=settings.ELASTICSEARCH_COLLECTION_INDEX, id=uuid, _source_includes=['manifest'])
        response = json.loads(response['_source']['manifest'])
        return JsonResponse(response)

    except NotFoundError:
        return Http404


