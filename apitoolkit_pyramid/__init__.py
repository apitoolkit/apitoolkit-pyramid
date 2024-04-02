import uuid
import requests
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from jsonpath_ng import parse
import json
import base64
import time
from datetime import datetime
import pytz
from apitoolkit_python import observe_request, report_error
from urllib.parse import urlsplit
from pyramid.request import Request


class APIToolkit(object):
    def __init__(self, handler, registry):
        print(registry, registry.settings)
        self.publisher = None
        self.topic_name = None
        self.meta = None
        self.redact_headers = registry.settings.get('APITOOLKIT_REDACT_HEADERS', [])
        self.debug = registry.settings.get('APITOOLKIT_DEBUG', False)
        self.redact_request_body = registry.settings.get('APITOOLKIT_REDACT_REQ_BODY', [])
        self.redact_response_body = registry.settings.get('APITOOLKIT_REDACT_RES_BODY', [])
        self.get_response = handler

        self.service_version = registry.settings.get("APITOOLKIT_SERVICE_VERSION", None)
        self.tags =registry.settings.get("APITOOLKIT_TAGS", [])

        api_key = registry.settings.get('APITOOLKIT_KEY')
        root_url = registry.settings.get('APITOOLKIT_ROOT_URL',
                           "https://app.apitoolkit.io")

        response = requests.get(url=root_url + "/api/client_metadata",
                                headers={"Authorization": f"Bearer {api_key}"})
        response.raise_for_status()
        data = response.json()
        credentials = service_account.Credentials.from_service_account_info(
            data["pubsub_push_service_account"])

        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=data['pubsub_project_id'],
            topic=data['topic_id'],
        )
        self.meta = data

    def getInfo(self):
        return {"project_id": self.meta["project_id"], "service_version": self.service_version, "tags": self.tags}

    def publish_message(self, payload):
        data = json.dumps(payload).encode('utf-8')
        if self.debug:
            print("APIToolkit: publish message")
            json_formatted_str = json.dumps(payload, indent=2)
            print(json_formatted_str)
        future = self.publisher.publish(self.topic_name, data=data)
        return future.result()

    def redact_headers_func(self, headers):
        redacted_headers = {}
        for header_name, value in headers.items():
            if header_name.lower() in self.redact_headers or header_name in self.redact_headers:
                redacted_headers[header_name] = "[CLIENT_REDACTED]"
            else:
                redacted_headers[header_name] = value
        return redacted_headers

    def redact_fields(self, body, paths):
        try:
            data = json.loads(body)
            for path in paths:
                expr = parse(path)
                expr.update(data, "[CLIENT_REDACTED]")
            return json.dumps(data).encode("utf-8")
        except Exception as e:
            if isinstance(body, str):
                return body.encode('utf-8')
            return body
    def process_exception(self, request, exception):
        report_error(request,exception)
        pass

    def __call__(self, request: Request):
        if self.debug:
            print("APIToolkit: making request")
        start_time = time.perf_counter_ns()
        request_method = request.method
        raw_url = request.url
        parsed_url = urlsplit(raw_url)
        url_path_with_query = parsed_url.path + parsed_url.query

        request_body = None
        query_params =  {key: value for key, value in request.params.items()}
        request_headers = self.redact_headers_func(request.headers)
        content_type = request.headers.get('Content-Type', '')

        if content_type == 'application/json':
            request_body = json.loads(request.body.decode('utf-8'))
        if content_type == 'text/plain':
            request_body = request.body.decode('utf-8')
        if content_type == 'application/x-www-form-urlencoded' or 'multipart/form-data' in content_type:
            request_body = dict(request.POST.copy())
        request.apitoolkit_message_id = str(uuid.uuid4())
        request.apitoolkit_errors = []
        request.apitoolkit_client = self

        response = self.get_response(request)

        if self.debug:
            print("APIToolkit: after request")
        end_time = time.perf_counter_ns()
        url_path = request.matched_route.pattern if request.matched_route is not None else request.path
        path_params = request.matchdict     
        duration = (end_time - start_time)
        status_code = response.status_code
        request_body = json.dumps(request_body)
        response_headers = self.redact_headers_func(response.headers)
        request_body = self.redact_fields(
            request_body, self.redact_request_body)
        response_body = self.redact_fields(
            response.body, self.redact_response_body)
        timestamp = datetime.now(pytz.timezone("UTC")).isoformat()
        message_id = request.apitoolkit_message_id
        errors = request.apitoolkit_errors
        try:
            payload = {
                "query_params": query_params,
                "path_params": path_params,
                "request_headers": request_headers,
                "response_headers": response_headers,
                "proto_minor": 1,
                "proto_major": 1,
                "method": request_method,
                "url_path": url_path,
                "raw_url": url_path_with_query ,
                "request_body": base64.b64encode(request_body).decode("utf-8"),
                "response_body": base64.b64encode(response_body).decode("utf-8"),
                "host": request.headers.get('HOST',""),
                "referer": request.headers.get('Referer', ""),
                "sdk_type": "PythonPyramid",
                "project_id": self.meta["project_id"],
                "status_code": status_code,
                "errors": errors,
                "msg_id": message_id,
                "parent_id": None,
                "duration": duration,
                "service_version": self.service_version,
                "tags": self.tags,
                "timestamp": timestamp
            }
            self.publish_message(payload)
        except Exception as e:
            return response
        return response