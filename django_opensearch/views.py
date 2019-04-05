from django.shortcuts import render
from django.views import View
from django.views.generic import TemplateView
from django.http import HttpResponse
from .opensearch.opensearch import OpensearchDescription, OpensearchResponse
import json
from django.conf import settings

# Create your views here.


class Index():
    pass


class Description(TemplateView):
    template_name = 'description.xml'
    # content_type = 'application/opensearchdescription+xml'
    content_type = 'application/xml'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        collectionId = self.request.GET.get("collectionId")

        context["osd"] = OpensearchDescription(collectionId)

        return context


class Response(TemplateView):
    template_name = 'response.xml'
    content_type = 'application/xml'


    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["osr"] = OpensearchResponse(self.request.GET)
        context["root_url"] = self.gen_item_root()

        return context

    def gen_item_root(self):

        return self.request.build_absolute_uri().split('?')[0]