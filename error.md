Run pytest -q
FF.                                                                      [100%]
=================================== FAILURES ===================================
___________________ test_process_event_accept_and_duplicate ____________________

self = <AWSHTTPConnection(host='minio', port=9000) at 0x7f1662e8f950>

    def _new_conn(self) -> socket.socket:
        """Establish a socket connection and set nodelay settings on it.
    
        :return: New socket connection.
        """
        try:
>           sock = connection.create_connection(
                (self._dns_host, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:204: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/util/connection.py:60: in create_connection
    for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

host = 'minio', port = 9000, family = <AddressFamily.AF_UNSPEC: 0>
type = <SocketKind.SOCK_STREAM: 1>, proto = 0, flags = 0

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        """Resolve host and port into list of address info entries.
    
        Translate the host/port argument into a sequence of 5-tuples that contain
        all the necessary arguments for creating a socket connected to that service.
        host is a domain name, a string representation of an IPv4/v6 address or
        None. port is a string service name such as 'http', a numeric port number or
        None. By passing None as the value of host and port, you can pass NULL to
        the underlying C API.
    
        The family, type and proto arguments can be optionally specified in order to
        narrow the list of addresses returned. Passing zero as a value for each of
        these arguments selects the full range of results.
        """
        # We override this function since we want to translate the numeric family
        # and socket type values to enum constants.
        addrlist = []
>       for res in _socket.getaddrinfo(host, port, family, type, proto, flags):
E       socket.gaierror: [Errno -3] Temporary failure in name resolution

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/socket.py:974: gaierror

The above exception was the direct cause of the following exception:

self = <botocore.httpsession.URLLib3Session object at 0x7f1662e530d0>
request = <AWSPreparedRequest stream_output=False, method=HEAD, url=http://minio:9000/datalake/raw/payment/dt%3D2026-01-03/evt-1...fda419c93e', 'amz-sdk-invocation-id': b'c5814f75-ff7d-4c08-9b5f-57c5e3e1a345', 'amz-sdk-request': b'attempt=5; max=5'}>

    def send(self, request):
        try:
            proxy_url = self._proxy_config.proxy_url_for(request.url)
            manager = self._get_connection_manager(request.url, proxy_url)
            conn = manager.connection_from_url(request.url)
            self._setup_ssl_cert(conn, request.url, self._verify)
            if ensure_boolean(
                os.environ.get('BOTO_EXPERIMENTAL__ADD_PROXY_HOST_HEADER', '')
            ):
                # This is currently an "experimental" feature which provides
                # no guarantees of backwards compatibility. It may be subject
                # to change or removal in any patch version. Anyone opting in
                # to this feature should strictly pin botocore.
                host = urlparse(request.url).hostname
                conn.proxy_headers['host'] = host
    
            request_target = self._get_request_target(request.url, proxy_url)
>           urllib_response = conn.urlopen(
                method=request.method,
                url=request_target,
                body=request.body,
                headers=request.headers,
                retries=Retry(False),
                assert_same_host=False,
                preload_content=False,
                decode_content=False,
                chunked=self._chunked(request.headers),
            )

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/httpsession.py:464: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connectionpool.py:841: in urlopen
    retries = retries.increment(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/util/retry.py:449: in increment
    raise reraise(type(error), error, _stacktrace)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/util/util.py:39: in reraise
    raise value
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connectionpool.py:787: in urlopen
    response = self._make_request(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connectionpool.py:493: in _make_request
    conn.request(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/awsrequest.py:96: in request
    rval = super().request(method, url, body, headers, *args, **kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:500: in request
    self.endheaders()
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/http/client.py:1298: in endheaders
    self._send_output(message_body, encode_chunked=encode_chunked)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/awsrequest.py:123: in _send_output
    self.send(msg)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/awsrequest.py:223: in send
    return super().send(str)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/http/client.py:996: in send
    self.connect()
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:331: in connect
    self.sock = self._new_conn()
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <AWSHTTPConnection(host='minio', port=9000) at 0x7f1662e8f950>

    def _new_conn(self) -> socket.socket:
        """Establish a socket connection and set nodelay settings on it.
    
        :return: New socket connection.
        """
        try:
            sock = connection.create_connection(
                (self._dns_host, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )
        except socket.gaierror as e:
>           raise NameResolutionError(self.host, self, e) from e
E           urllib3.exceptions.NameResolutionError: AWSHTTPConnection(host='minio', port=9000): Failed to resolve 'minio' ([Errno -3] Temporary failure in name resolution)

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:211: NameResolutionError

During handling of the above exception, another exception occurred:

monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7f1665970910>

    def test_process_event_accept_and_duplicate(monkeypatch):
        # Patch S3 uploader
        dummy = DummyS3()
        monkeypatch.setattr("platform_common.s3.put_json", dummy.put_json)
    
        session = make_session()
    
        payload = {
            "event_id": "evt-1",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "customer_id": "cust-1",
            "region": "us-east",
            "amount": 100.0,
            "currency": "USD",
        }
    
>       res1 = process_event(session, "payment", payload)

tests/test_ingestion.py:46: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
ingestion/app/service.py:76: in process_event
    put_json(key, json.loads(obj.model_dump_json()))
platform_common/s3.py:55: in put_json
    if object_exists(key, bucket_name):
platform_common/s3.py:42: in object_exists
    client.head_object(Bucket=bucket_name, Key=key)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/client.py:565: in _api_call
    return self._make_api_call(operation_name, kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/client.py:999: in _make_api_call
    http, parsed_response = self._make_request(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/client.py:1023: in _make_request
    return self._endpoint.make_request(operation_model, request_dict)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:119: in make_request
    return self._send_request(request_dict, operation_model)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:200: in _send_request
    while self._needs_retry(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:352: in _needs_retry
    responses = self._event_emitter.emit(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/hooks.py:412: in emit
    return self._emitter.emit(aliased_event_name, **kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/hooks.py:256: in emit
    return self._emit(event_name, kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/hooks.py:239: in _emit
    response = handler(**kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:207: in __call__
    if self._checker(**checker_kwargs):
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:284: in __call__
    should_retry = self._should_retry(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:320: in _should_retry
    return self._checker(attempt_number, response, caught_exception)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:363: in __call__
    checker_response = checker(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:247: in __call__
    return self._check_caught_exception(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:416: in _check_caught_exception
    raise caught_exception
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:279: in _do_get_response
    http_response = self._send(request)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:375: in _send
    return self.http_session.send(request)
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <botocore.httpsession.URLLib3Session object at 0x7f1662e530d0>
request = <AWSPreparedRequest stream_output=False, method=HEAD, url=http://minio:9000/datalake/raw/payment/dt%3D2026-01-03/evt-1...fda419c93e', 'amz-sdk-invocation-id': b'c5814f75-ff7d-4c08-9b5f-57c5e3e1a345', 'amz-sdk-request': b'attempt=5; max=5'}>

    def send(self, request):
        try:
            proxy_url = self._proxy_config.proxy_url_for(request.url)
            manager = self._get_connection_manager(request.url, proxy_url)
            conn = manager.connection_from_url(request.url)
            self._setup_ssl_cert(conn, request.url, self._verify)
            if ensure_boolean(
                os.environ.get('BOTO_EXPERIMENTAL__ADD_PROXY_HOST_HEADER', '')
            ):
                # This is currently an "experimental" feature which provides
                # no guarantees of backwards compatibility. It may be subject
                # to change or removal in any patch version. Anyone opting in
                # to this feature should strictly pin botocore.
                host = urlparse(request.url).hostname
                conn.proxy_headers['host'] = host
    
            request_target = self._get_request_target(request.url, proxy_url)
            urllib_response = conn.urlopen(
                method=request.method,
                url=request_target,
                body=request.body,
                headers=request.headers,
                retries=Retry(False),
                assert_same_host=False,
                preload_content=False,
                decode_content=False,
                chunked=self._chunked(request.headers),
            )
    
            http_response = botocore.awsrequest.AWSResponse(
                request.url,
                urllib_response.status,
                urllib_response.headers,
                urllib_response,
            )
    
            if not request.stream_output:
                # Cause the raw stream to be exhausted immediately. We do it
                # this way instead of using preload_content because
                # preload_content will never buffer chunked responses
                http_response.content
    
            return http_response
        except URLLib3SSLError as e:
            raise SSLError(endpoint_url=request.url, error=e)
        except (NewConnectionError, socket.gaierror) as e:
>           raise EndpointConnectionError(endpoint_url=request.url, error=e)
E           botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://minio:9000/datalake/raw/payment/dt%3D2026-01-03/evt-1.json"

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/httpsession.py:493: EndpointConnectionError
_______________________ test_process_event_late_arrival ________________________

self = <AWSHTTPConnection(host='minio', port=9000) at 0x7f16633b0310>

    def _new_conn(self) -> socket.socket:
        """Establish a socket connection and set nodelay settings on it.
    
        :return: New socket connection.
        """
        try:
>           sock = connection.create_connection(
                (self._dns_host, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:204: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/util/connection.py:60: in create_connection
    for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

host = 'minio', port = 9000, family = <AddressFamily.AF_UNSPEC: 0>
type = <SocketKind.SOCK_STREAM: 1>, proto = 0, flags = 0

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        """Resolve host and port into list of address info entries.
    
        Translate the host/port argument into a sequence of 5-tuples that contain
        all the necessary arguments for creating a socket connected to that service.
        host is a domain name, a string representation of an IPv4/v6 address or
        None. port is a string service name such as 'http', a numeric port number or
        None. By passing None as the value of host and port, you can pass NULL to
        the underlying C API.
    
        The family, type and proto arguments can be optionally specified in order to
        narrow the list of addresses returned. Passing zero as a value for each of
        these arguments selects the full range of results.
        """
        # We override this function since we want to translate the numeric family
        # and socket type values to enum constants.
        addrlist = []
>       for res in _socket.getaddrinfo(host, port, family, type, proto, flags):
E       socket.gaierror: [Errno -3] Temporary failure in name resolution

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/socket.py:974: gaierror

The above exception was the direct cause of the following exception:

self = <botocore.httpsession.URLLib3Session object at 0x7f1662dfd750>
request = <AWSPreparedRequest stream_output=False, method=HEAD, url=http://minio:9000/datalake/raw/payment/dt%3D2025-12-24/evt-l...8d5455ef72', 'amz-sdk-invocation-id': b'06527a9a-3307-4234-af6c-49ce0ecd07fc', 'amz-sdk-request': b'attempt=5; max=5'}>

    def send(self, request):
        try:
            proxy_url = self._proxy_config.proxy_url_for(request.url)
            manager = self._get_connection_manager(request.url, proxy_url)
            conn = manager.connection_from_url(request.url)
            self._setup_ssl_cert(conn, request.url, self._verify)
            if ensure_boolean(
                os.environ.get('BOTO_EXPERIMENTAL__ADD_PROXY_HOST_HEADER', '')
            ):
                # This is currently an "experimental" feature which provides
                # no guarantees of backwards compatibility. It may be subject
                # to change or removal in any patch version. Anyone opting in
                # to this feature should strictly pin botocore.
                host = urlparse(request.url).hostname
                conn.proxy_headers['host'] = host
    
            request_target = self._get_request_target(request.url, proxy_url)
>           urllib_response = conn.urlopen(
                method=request.method,
                url=request_target,
                body=request.body,
                headers=request.headers,
                retries=Retry(False),
                assert_same_host=False,
                preload_content=False,
                decode_content=False,
                chunked=self._chunked(request.headers),
            )

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/httpsession.py:464: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connectionpool.py:841: in urlopen
    retries = retries.increment(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/util/retry.py:449: in increment
    raise reraise(type(error), error, _stacktrace)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/util/util.py:39: in reraise
    raise value
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connectionpool.py:787: in urlopen
    response = self._make_request(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connectionpool.py:493: in _make_request
    conn.request(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/awsrequest.py:96: in request
    rval = super().request(method, url, body, headers, *args, **kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:500: in request
    self.endheaders()
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/http/client.py:1298: in endheaders
    self._send_output(message_body, encode_chunked=encode_chunked)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/awsrequest.py:123: in _send_output
    self.send(msg)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/awsrequest.py:223: in send
    return super().send(str)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/http/client.py:996: in send
    self.connect()
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:331: in connect
    self.sock = self._new_conn()
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <AWSHTTPConnection(host='minio', port=9000) at 0x7f16633b0310>

    def _new_conn(self) -> socket.socket:
        """Establish a socket connection and set nodelay settings on it.
    
        :return: New socket connection.
        """
        try:
            sock = connection.create_connection(
                (self._dns_host, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )
        except socket.gaierror as e:
>           raise NameResolutionError(self.host, self, e) from e
E           urllib3.exceptions.NameResolutionError: AWSHTTPConnection(host='minio', port=9000): Failed to resolve 'minio' ([Errno -3] Temporary failure in name resolution)

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/urllib3/connection.py:211: NameResolutionError

During handling of the above exception, another exception occurred:

monkeypatch = <_pytest.monkeypatch.MonkeyPatch object at 0x7f16626406d0>

    def test_process_event_late_arrival(monkeypatch):
        dummy = DummyS3()
        monkeypatch.setattr("platform_common.s3.put_json", dummy.put_json)
    
        session = make_session()
    
        old_time = datetime.now(timezone.utc) - timedelta(days=10)
        payload = {
            "event_id": "evt-late",
            "event_time": old_time.isoformat(),
            "customer_id": "cust-1",
            "region": "us-east",
            "amount": 5.0,
            "currency": "USD",
        }
    
>       res = process_event(session, "payment", payload)

tests/test_ingestion.py:70: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
ingestion/app/service.py:76: in process_event
    put_json(key, json.loads(obj.model_dump_json()))
platform_common/s3.py:55: in put_json
    if object_exists(key, bucket_name):
platform_common/s3.py:42: in object_exists
    client.head_object(Bucket=bucket_name, Key=key)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/client.py:565: in _api_call
    return self._make_api_call(operation_name, kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/client.py:999: in _make_api_call
    http, parsed_response = self._make_request(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/client.py:1023: in _make_request
    return self._endpoint.make_request(operation_model, request_dict)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:119: in make_request
    return self._send_request(request_dict, operation_model)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:200: in _send_request
    while self._needs_retry(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:352: in _needs_retry
    responses = self._event_emitter.emit(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/hooks.py:412: in emit
    return self._emitter.emit(aliased_event_name, **kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/hooks.py:256: in emit
    return self._emit(event_name, kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/hooks.py:239: in _emit
    response = handler(**kwargs)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:207: in __call__
    if self._checker(**checker_kwargs):
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:284: in __call__
    should_retry = self._should_retry(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:320: in _should_retry
    return self._checker(attempt_number, response, caught_exception)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:363: in __call__
    checker_response = checker(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:247: in __call__
    return self._check_caught_exception(
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/retryhandler.py:416: in _check_caught_exception
    raise caught_exception
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:279: in _do_get_response
    http_response = self._send(request)
/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/endpoint.py:375: in _send
    return self.http_session.send(request)
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <botocore.httpsession.URLLib3Session object at 0x7f1662dfd750>
request = <AWSPreparedRequest stream_output=False, method=HEAD, url=http://minio:9000/datalake/raw/payment/dt%3D2025-12-24/evt-l...8d5455ef72', 'amz-sdk-invocation-id': b'06527a9a-3307-4234-af6c-49ce0ecd07fc', 'amz-sdk-request': b'attempt=5; max=5'}>

    def send(self, request):
        try:
            proxy_url = self._proxy_config.proxy_url_for(request.url)
            manager = self._get_connection_manager(request.url, proxy_url)
            conn = manager.connection_from_url(request.url)
            self._setup_ssl_cert(conn, request.url, self._verify)
            if ensure_boolean(
                os.environ.get('BOTO_EXPERIMENTAL__ADD_PROXY_HOST_HEADER', '')
            ):
                # This is currently an "experimental" feature which provides
                # no guarantees of backwards compatibility. It may be subject
                # to change or removal in any patch version. Anyone opting in
                # to this feature should strictly pin botocore.
                host = urlparse(request.url).hostname
                conn.proxy_headers['host'] = host
    
            request_target = self._get_request_target(request.url, proxy_url)
            urllib_response = conn.urlopen(
                method=request.method,
                url=request_target,
                body=request.body,
                headers=request.headers,
                retries=Retry(False),
                assert_same_host=False,
                preload_content=False,
                decode_content=False,
                chunked=self._chunked(request.headers),
            )
    
            http_response = botocore.awsrequest.AWSResponse(
                request.url,
                urllib_response.status,
                urllib_response.headers,
                urllib_response,
            )
    
            if not request.stream_output:
                # Cause the raw stream to be exhausted immediately. We do it
                # this way instead of using preload_content because
                # preload_content will never buffer chunked responses
                http_response.content
    
            return http_response
        except URLLib3SSLError as e:
            raise SSLError(endpoint_url=request.url, error=e)
        except (NewConnectionError, socket.gaierror) as e:
>           raise EndpointConnectionError(endpoint_url=request.url, error=e)
E           botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://minio:9000/datalake/raw/payment/dt%3D2025-12-24/evt-late.json"

/opt/hostedtoolcache/Python/3.11.14/x64/lib/python3.11/site-packages/botocore/httpsession.py:493: EndpointConnectionError
=========================== short test summary info ============================
FAILED tests/test_ingestion.py::test_process_event_accept_and_duplicate - botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://minio:9000/datalake/raw/payment/dt%3D2026-01-03/evt-1.json"
FAILED tests/test_ingestion.py::test_process_event_late_arrival - botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://minio:9000/datalake/raw/payment/dt%3D2025-12-24/evt-late.json"
Error: Process completed with exit code 1.