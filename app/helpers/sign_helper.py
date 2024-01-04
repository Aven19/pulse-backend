"""
ProductAdvertisingAPI Signed Url Helper

https://webservices.amazon.com/paapi5/documentation/index.html

"""

import hashlib
import hmac
import json
from typing import Dict

from app import logger


class AWSV4Auth:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        host: str,
        region: str,
        service: str,
        method_name: str,
        timestamp: str,
        headers: Dict[str, str] = {},
        path: str='',
        payload: str='',
    ):
        """
        Initialize the AWSV4Auth object with required parameters.

        Args:
            access_key (str): The AWS access key.
            secret_key (str): The AWS secret key.
            host (str): The AWS service host.
            region (str): The AWS region.
            service (str): The AWS service.
            method_name (str): The HTTP method name (e.g., 'GET', 'POST').
            timestamp (datetime): The timestamp for the request.
            headers (dict): Optional headers for the request.
            path (str): The request path.
            payload (str): The request payload.
        """

        self.access_key = access_key
        self.secret_key = secret_key
        self.host = host
        self.region = region
        self.service = service
        self.method_name = method_name
        self.headers = headers
        self.timestamp = timestamp
        self.payload = payload
        self.path = path

        # Date and time stamp
        self.xAmzDateTime = self.timestamp.strftime('%Y%m%dT%H%M%SZ')
        self.xAmzDate = self.timestamp.strftime('%Y%m%d')

        logger.info(self.host)
        logger.info(self.region)
        logger.info(self.service)
        logger.info(self.method_name)
        logger.info(self.headers)
        logger.info(self.timestamp)
        logger.info(self.payload)
        logger.info(self.path)
        logger.info(self.xAmzDateTime)
        logger.info(self.xAmzDate)

    def get_headers(self):
        """
        Generate the headers required for AWS V4 authentication.

        Returns:
            dict: A dictionary of headers, including the Authorization header.
        """
        canonical_request = self.prepare_canonical_url()
        string_to_sign = self.prepare_string_to_sign(
            canonical_request=canonical_request
        )
        logger.info(f'AWSV4Auth.get_headers(): {self.xAmzDate}')

        signing_key = self.get_signature_key(
            self.secret_key, self.xAmzDate, self.region, self.service
        )
        signature = self.get_signature(
            signing_key=signing_key, string_to_sign=string_to_sign
        )

        authorization_header = (
            self.algorithm
            + ' '
            + 'Credential='
            + self.access_key
            + '/'
            + self.credential_scope
            + ', '
            + 'SignedHeaders='
            + self.signed_header
            + ', '
            + 'Signature='
            + signature
        )
        self.headers['Authorization'] = authorization_header
        return self.headers

    def prepare_canonical_url(self):
        """
        Prepare the canonical URL for the AWS request.

        Returns:
            str: The canonical URL.
        """
        canonical_uri = self.method_name + '\n' + self.path
        canonical_querystring = ''
        canonical_header = ''
        self.signed_header = ''
        sorted_keys = sorted(self.headers, key=str.lower)
        for key in sorted_keys:
            self.signed_header = self.signed_header + key.lower() + ';'
            canonical_header = (
                canonical_header + key.lower() + ':' + self.headers[key] + '\n'
            )
        self.signed_header = self.signed_header[:-1]
        payload_hash = hashlib.sha256(
            json.dumps(self.payload).encode('utf-8')
        ).hexdigest()
        canonical_request = (
            canonical_uri
            + '\n'
            + canonical_querystring
            + '\n'
            + canonical_header
            + '\n'
            + self.signed_header
            + '\n'
            + payload_hash
        )
        return canonical_request

    def prepare_string_to_sign(self, canonical_request):
        """
        Prepare the string to sign for AWS V4 authentication.

        Args:
            canonical_request (str): The canonical request.

        Returns:
            str: The string to sign.
        """
        self.algorithm = 'AWS4-HMAC-SHA256'
        self.credential_scope = (
            self.xAmzDate
            + '/'
            + self.region
            + '/'
            + self.service
            + '/'
            + 'aws4_request'
        )
        string_to_sign = (
            self.algorithm
            + '\n'
            + self.xAmzDateTime
            + '\n'
            + self.credential_scope
            + '\n'
            + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        )
        return string_to_sign

    def sign(self, key, msg):
        """
        Sign a message using HMAC-SHA256.

        Args:
            key (bytes): The signing key.
            msg (str): The message to sign.

        Returns:
            bytes: The signature.
        """

        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def get_signature_key(self, key, date_stamp, region_name, service_name):
        """
        Generate the signing key for AWS V4 authentication.

        Args:
            key (str): The AWS secret key.
            date_stamp (str): The date stamp.
            region_name (str): The AWS region name.
            service_name (str): The AWS service name.

        Returns:
            bytes: The signing key.
        """

        k_date = self.sign(('AWS4' + key).encode('utf-8'), date_stamp)
        k_region = self.sign(k_date, region_name)
        k_service = self.sign(k_region, service_name)
        k_signing = self.sign(k_service, 'aws4_request')
        return k_signing

    def get_signature(self, signing_key, string_to_sign):
        """
        Generate the signature for AWS V4 authentication.

        Args:
            signing_key (bytes): The signing key.
            string_to_sign (str): The string to sign.

        Returns:
            str: The signature.
        """
        signature = hmac.new(
            signing_key, string_to_sign.encode('utf-8'), hashlib.sha256
        ).hexdigest()
        return signature
