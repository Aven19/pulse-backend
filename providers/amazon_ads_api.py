from abc import abstractmethod
from datetime import datetime

from app import logger
from app.helpers.constants import ASpApiBaseURL
from app.helpers.utility import generate_seller_api_awssigv4
import requests
# pytype: disable=ignored-abstractmethod


class AbstractAmazonAdsReport:

    def __init__(self, credentials):
        self.credentials = credentials
        self.access_token = None

    @abstractmethod
    def get_access_token(self):
        pass

    @abstractmethod
    def create_report(self, payload):
        pass

    @abstractmethod
    def verify_report(self, report_id):
        pass

    @abstractmethod
    def retrieve_report(self, report_id):
        pass

    @abstractmethod
    def retrieve_report_download(self, report_id):
        pass

    @abstractmethod
    def create_report_v2(self, payload):
        pass

    @abstractmethod
    def verify_report_v2(self, report_id):
        pass

    @abstractmethod
    def retrieve_report_v2(self, report_id):
        pass

    @abstractmethod
    def retrieve_report_download_v2(self, report_id):
        pass


class AmazonAdsReportEU(AbstractAmazonAdsReport):

    def __init__(self, credentials):
        super().__init__(credentials)
        self.generate_access_token()

    def generate_access_token(self):
        """Generates the access token for the Amazon report API."""

        if self.access_token is None:

            # cache_key = RedisCacheKeys.AMAZON_ACCESS_TOKEN.value
            # cached_object = r.get(cache_key)

            # if cached_object:
            #     cached_result = pickle.loads(cached_object)
            #     if cached_result.get('seller_partner_id') == seller_partner_id:
            #         self.access_token = cached_result['access_token']

            url = 'https://api.amazon.co.uk/auth/o2/token'
            # url = 'https://api.amazon.com/auth/o2/token'

            payload = {
                'grant_type': 'refresh_token',
                'refresh_token': self.credentials['refresh_token'],
                'client_id': self.credentials['client_id'],
                'client_secret': self.credentials['client_secret']
            }

            response = requests.request('POST', url, data=payload)
            result = response.json()

            # Add Seller partner object to result
            # result['seller_partner_id'] = seller_partner_id

            self.access_token = result['access_token']

            # r.set(name=cache_key, value=pickle.dumps(result), ex=TimeInSeconds.SIXTY_MIN.value)

    def create_report(self, payload):
        """Creates a report."""

        url = 'https://advertising-api-eu.amazon.com/reporting/reports'

        headers = {
            'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
            'Amazon-Advertising-API-ClientId': self.credentials['client_id'],
            'Amazon-Advertising-API-Scope': self.credentials['az_ads_profile_id'],
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.post(url, headers=headers, json=payload)
        result = response.json()

        return result

    def verify_report(self, report_id):
        """Verifies the report."""

        url = f'https://advertising-api-eu.amazon.com/reporting/reports/{report_id}'
        headers = {
            'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
            'Amazon-Advertising-API-ClientId': self.credentials['client_id'],
            'Amazon-Advertising-API-Scope': self.credentials['az_ads_profile_id'],
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.request('GET', url, headers=headers)
        result = response.json()

        return result

    def retrieve_report(self, document_id):
        """Retrieves the data from the document id."""
        url = ASpApiBaseURL.IN.value + '/reports/2021-06-30/documents/' + document_id

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers)
        result = response.json()

        return result

    def retrieve_report_download(self, report_id):
        """Verifies the report."""

        url = f'https://advertising-api-eu.amazon.com/reporting/reports/{report_id}'
        headers = {
            'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
            'Amazon-Advertising-API-ClientId': self.credentials['client_id'],
            'Amazon-Advertising-API-Scope': self.credentials['az_ads_profile_id'],
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.request('GET', url, headers=headers)

        result = response.json()

        return result

    def create_report_v2(self, payload, url):
        """Creates a report."""

        headers = {
            'Content-Type': 'application/json',
            'Amazon-Advertising-API-ClientId': self.credentials['client_id'],
            'Amazon-Advertising-API-Scope': self.credentials['az_ads_profile_id'],
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.post(url, headers=headers, json=payload)
        result = response.json()

        # Dump the response headers
        logger.info('*' * 150)
        logger.info('Amazon AD Create Report Version 2, Response Headers:')
        for header, value in response.headers.items():
            logger.info(f'{header}: {value}')

        # Dump the response status and response code
        logger.info(f'Response Status: {response.status_code}')
        logger.info(f'Response Code: {response.status_code}')

        rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
        logger.info(rate_limit)

        return result

    def verify_report_v2(self, document_id):
        """Verifies the report."""

        url = f'https://advertising-api-eu.amazon.com/v2/reports/{document_id}'

        headers = {
            'Content-Type': 'application/json',
            'Amazon-Advertising-API-ClientId': self.credentials['client_id'],
            'Amazon-Advertising-API-Scope': self.credentials['az_ads_profile_id'],
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.request('GET', url, headers=headers)
        result = response.json()

        return result

    def retrieve_report_v2(self, document_id):
        """Retrieves the data from the document id."""
        url = ASpApiBaseURL.IN.value + '/reports/2021-06-30/documents/' + document_id

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers)
        result = response.json()

        return result

    def retrieve_report_download_v2(self, document_id):
        """To get download output for display report."""

        url = f'https://advertising-api-eu.amazon.com/v1/reports/{document_id}/download'

        headers = {
            'Content-Type': 'application/json',
            'Amazon-Advertising-API-ClientId': self.credentials['client_id'],
            'Amazon-Advertising-API-Scope': self.credentials['az_ads_profile_id'],
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.request('GET', url, headers=headers)

        return response
