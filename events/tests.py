import datetime
import json

from django.contrib.auth.models import User
from django.test import TestCase
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase
from rest_framework.test import RequestsClient

from events.models import Event

# CONSTANTS

MULTI_ITEM_JSON = [
    {
        "collection_id": "1",
        "collection_title": "lorem ipsum",
        "action": "added",
        "datetime": datetime.datetime.now().isoformat(),
    },
    {
        "collection_id": "2",
        "collection_title": "dummy text",
        "action": "removed",
        "datetime": datetime.datetime.now().isoformat(),
    },
    {
        "collection_id": "3",
        "collection_title": "abc",
        "action": "updated",
        "datetime": datetime.datetime.now().isoformat(),
    },
    {
        "collection_id": "4",
        "collection_title": "igpay atinlay",
        "action": "added",
        "datetime": "2000-11-01T00:00:00.00",
    },
]
URL = "http://testserver/api/events/"


class AuthenticationTests(APITestCase, TestCase):
    def setUp(self):
        self.client = RequestsClient()
        self.user = User.objects.create_user(username="testuser", password="P455w0rd")
        token, created = Token.objects.get_or_create(user=self.user)
        self.key = token.key
        self.url = URL
        Event.objects.create(
            collection_id="0",
            collection_title="blank",
            action="added",
            datetime="2000-11-01",
        )

    def test_anonymous_get(self):
        """
        Test anonymous user can GET request successfully
        """
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            [
                {
                    "collection_id": "0",
                    "collection_title": "blank",
                    "action": "added",
                    "datetime": "2000-11-01",
                }
            ],
        )

    def test_unauthenticated_post(self):
        """
        Test unauthenticated users are 401 Forbidden to POST request
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = json.dumps(MULTI_ITEM_JSON, indent=4)
        response = self.client.post(self.url, data=data, headers=headers)

        self.assertEqual(response.status_code, 401)

    def test_authenticated_post(self):
        """
        Test authenticated users are allowed to POST request with 201 Create
        """
        headers = {
            "Authorization": f"Token {self.key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = json.dumps(MULTI_ITEM_JSON, indent=4)
        response = self.client.post(self.url, data=data, headers=headers)

        self.assertEqual(response.status_code, 201)


class SerializerTests(APITestCase, TestCase):
    def setUp(self):
        self.client = RequestsClient()
        self.user = User.objects.create_user(username="testuser", password="P455w0rd")
        token, created = Token.objects.get_or_create(user=self.user)
        self.key = token.key
        self.url = URL
        self.headers = {
            "Authorization": f"Token {self.key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def test_json_to_model(self):
        """
        Test JSON object is successfully and accurately converted to Events Model Object
        """
        data = [
            {
                "collection_id": "0",
                "collection_title": "blank",
                "action": "added",
                "datetime": datetime.datetime.now().isoformat(),
            }
        ]
        data = json.dumps(data, indent=4)
        response = self.client.post(self.url, data=data, headers=self.headers)
        model = Event.objects.all().first()
        post_json = response.json()[0]
        self.assertTrue(Event.objects.all())
        self.assertEqual(model.collection_title, post_json.get("collection_title"))
        self.assertEqual(model.collection_id, post_json.get("collection_id"))
        self.assertEqual(model.datetime.strftime("%Y-%m-%d"), post_json.get("datetime"))

    def test_invalid_format_request(self):
        """
        Test invalid JSON of wrong format returns 400 Bad Request
        """
        data = [{"invalid": "format"}]
        data = json.dumps(data, indent=4)
        response = self.client.post(self.url, data=data, headers=self.headers)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            [
                {
                    "collection_id": ["This field is required."],
                    "collection_title": ["This field is required."],
                    "action": ["This field is required."],
                    "datetime": ["This field is required."],
                }
            ],
        )

    def test_not_iso_datetime(self):
        """
        Test if datetime in JSON is not ISO-8601 then return 400 Bad Request
        """
        data = [
            {
                "collection_id": "1",
                "collection_title": "title",
                "action": "updated",
                "datetime": "2000-11-01",
            }
        ]
        data = json.dumps(data, indent=4)
        response = self.client.post(self.url, data=data, headers=self.headers)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            [
                {
                    "datetime": [
                        "Date has wrong format. Use one of these formats instead: "
                        "YYYY-MM-DDThh:mm:ss.uuuuuu."
                    ]
                }
            ],
        )


class QuerySearchTests(APITestCase, TestCase):
    def setUp(self):
        self.client = RequestsClient()
        user = User.objects.create_user(username="testuser", password="P455w0rd")
        token, created = Token.objects.get_or_create(user=user)
        key = token.key
        self.url = URL
        headers = {
            "Authorization": f"Token {key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = json.dumps(MULTI_ITEM_JSON, indent=4)
        self.client.post(self.url, data=data, headers=headers)

    def test_simple_queries(self):
        """
        Test each query filter and return expected number of models returned that match query
        """
        response_id = self.client.get(self.url + "?collection_id=1")
        response_date = self.client.get(self.url + "?from_date=2020-01-01")
        response_action = self.client.get(self.url + "?action=added")
        response_none = self.client.get(self.url + "?collection_id=0")

        self.assertEqual(len(response_id.json()), 1)
        self.assertEqual(len(response_action.json()), 2)
        self.assertEqual(len(response_date.json()), 3)
        self.assertFalse(response_none.json())

    def test_multiple_queries(self):
        """
        Test multiple queries in one filter and return expected number of models that match all queries
        """
        url = (
            self.url + f'?from_date={datetime.datetime.now().strftime("%Y-%m-%d")}'
            f"&action=added"
        )
        response = self.client.get(url)

        self.assertEqual(len(response.json()), 1)
