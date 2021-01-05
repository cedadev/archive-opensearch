from django.views.generic import TemplateView


class Index(TemplateView):
    """
    Simple index page view which directs to the opensearch description
    """
    template_name = 'search/index.html'
