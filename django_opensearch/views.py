from django.shortcuts import render
from django.views import View
from django.views.generic import TemplateView
from django.views.generic.base import ContextMixin
from django.http import HttpResponse
from .opensearch.opensearch import OpensearchDescription, OpensearchResponse
import jsonpickle

jsonpickle.set_encoder_options('json', indent=4)
from django.conf import settings
from django_opensearch import settings as opensearch_settings


# Create your views here.


class Index():
    pass


class Description(TemplateView):
    template_name = 'description.xml'
    content_type = 'application/xml'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        context["osd"] = OpensearchDescription(self.request)

        return context


class Response(ContextMixin, View):
    def get(self, request):
        context = self.get_context_data()

        # Get accept params
        accept_param = request.GET.get('httpAccept')

        try:
            accept_header = request.headers.get('Accept')
        except AttributeError:
            accept_header = None

        if accept_param:
            response_type = accept_param

        else:
            if not accept_header:
                accept_header = opensearch_settings.DEFAULT_RESPONSE_TYPE

            response_type = accept_header

        # Pick the template
        if response_type in opensearch_settings.RESPONSE_TYPES:

            if response_type == 'application/atom+xml':
                return render(request, 'response.xml', context, content_type='application/xml')

            if response_type == 'application/geo+json':
                return HttpResponse(jsonpickle.encode(context['osr'], unpicklable=False), content_type='application/json')

        # Response type not found
        return HttpResponse(f'Accept parameter: {response_type} cannot be provided by this service. Possible response types: {opensearch_settings.RESPONSE_TYPES}',status=406)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["osr"] = OpensearchResponse(self.request)
        context["root_url"] = self.gen_item_root()

        return context

    def gen_item_root(self):

        return self.request.build_absolute_uri().split('?')[0]



