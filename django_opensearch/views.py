from django.shortcuts import render
from django.views import View
from django.views.generic import TemplateView
from django.views.generic.base import ContextMixin
from django.http import HttpResponse
from .opensearch.opensearch import OpensearchDescription, OpensearchResponse
import json
import jsonpickle

jsonpickle.set_encoder_options('json', indent=4)
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


class Response(ContextMixin, View):
    def get(self, request, response_type):
        context = self.get_context_data()

        if response_type == 'atom':
            return render(request, 'response.xml', context, content_type='application/xml')

        if response_type == 'json+geo':
            return HttpResponse(jsonpickle.encode(context['osr']), content_type='application/geo+json')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["osr"] = OpensearchResponse(self.request.GET)
        context["root_url"] = self.gen_item_root()

        return context

    def gen_item_root(self):

        return self.request.build_absolute_uri().split('?')[0]



