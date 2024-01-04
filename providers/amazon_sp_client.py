from abc import abstractmethod
from datetime import datetime
import pickle
import time
from urllib.parse import quote_plus

from app import logger
from app import r
from app.helpers.constants import ASpApiBaseURL
from app.helpers.constants import RedisCacheKeys
from app.helpers.constants import TimeInSeconds
from app.helpers.utility import generate_seller_api_awssigv4
import requests
# pytype: disable=ignored-abstractmethod
class AbstractAmazonReport:

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
    def get_scheduled_report(self, params):
        pass

    @abstractmethod
    def get_rdt_token(self, payload):
        pass

    @abstractmethod
    def get_orders(self, params):
        pass

    @abstractmethod
    def get_next_page_result(self, params):
        pass

    @abstractmethod
    def get_my_fees_estimate_for_sku(self, payload, seller_sku):
        pass

    @abstractmethod
    def get_fees_estimates(self, payload, asin):
        pass

    @abstractmethod
    def get_catalog_items(self, params):
        pass

    @abstractmethod
    def get_sales(self, params, web_call=False):
        pass

    @abstractmethod
    def get_fba_inventory(self, params):
        pass


class AmazonReportEU(AbstractAmazonReport):

    def __init__(self, credentials):
        super().__init__(credentials)
        self.generate_access_token()

    def generate_access_token(self):
        """Generates the access token for the Amazon report API."""

        logger.info('*' * 200)
        logger.info('Line 76: Access token: \n{access_token}'.format(access_token=self.access_token))
        logger.info('Line 77: Credentials: \n{credentials}'.format(credentials=self.credentials))

        seller_partner_id = self.credentials['seller_partner_id']

        if self.access_token is None:

            cache_key = f'{RedisCacheKeys.AMAZON_ACCESS_TOKEN.value}_{seller_partner_id}'
            logger.info(f'Line 84: AMAZON_ACCESS_TOKEN Key\n{cache_key}')

            cached_object = r.get(cache_key)

            if cached_object:
                cached_result = pickle.loads(cached_object)


                if cached_result.get('seller_partner_id') == seller_partner_id:
                    logger.info('Line 93: Cached Seller Partner Id: {asp_id}'.format(asp_id=cached_result['seller_partner_id']))
                    logger.info('Line 94: Cached Access token: \n{cached_access_token}'.format(cached_access_token=cached_result['access_token']))

                    self.access_token = cached_result['access_token']
                    logger.info('Line 97: Access token from Cached Result: \n{access_token}'.format(access_token=self.access_token))


            if self.access_token is None:

                url = 'https://api.amazon.com/auth/o2/token'

                payload = {
                    'grant_type': 'refresh_token',
                    'refresh_token': self.credentials['refresh_token'],
                    'client_id': self.credentials['client_id'],
                    'client_secret': self.credentials['client_secret']
                }

                response = requests.request('POST', url, data=payload)
                result = response.json()

                logger.info(f'Auth token Url: {url}')
                logger.info('Payload: \n{_payload}'.format(_payload=payload))
                logger.info('Response: \n{_response}'.format(_response=response))
                logger.info('Result: \n{_result}'.format(_result=result))

                #Add Seller partner object to result
                result['seller_partner_id'] = seller_partner_id

                self.access_token = result['access_token']

                r.set(name=cache_key, value=pickle.dumps(result), ex=TimeInSeconds.SIXTY_MIN.value)

    def create_report(self, payload, _response_headers=False, _rate_limit=False):
        """Creates an order report."""

        logger.info('Line 130: Access token for create report: \n{access_token}'.format(access_token=self.access_token))

        url = ASpApiBaseURL.IN.value+'/reports/2021-06-30/reports'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('POST', url, auth=aws_auth, headers=headers, json=payload)
        result = response.json()

        # Dump the response headers
        logger.info('*' * 150)
        logger.info('Amazon SP Client create_report, Response Headers:')
        response_headers = {}
        for header, value in response.headers.items():
            response_headers[header] = value
            logger.info(f'{header}: {value}')

        # Dump the response status and response code
        response_code = response.status_code
        logger.info(f'Response Status: {response.status_code}')
        logger.info(f'Response Code: {response.status_code}')

        rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
        logger.info(rate_limit)

        """A 429 is a retry-able status code, Read the x-amzn-RateLimit-Limit header, when available."""
        if response_code == 429 and rate_limit is None:
            logger.info('Rate limit is None. Retrying in 300 seconds...')
            time.sleep(300)
            return self.create_report(payload=payload)

        if _response_headers or _rate_limit:
            return result, response_headers, rate_limit

        return result

    def verify_report(self, report_id):
        """Verifies an order report."""

        url = ASpApiBaseURL.IN.value+'/reports/2021-06-30/reports/' + report_id

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers)
        result = response.json()

        return result

    def retrieve_report(self, document_id):
        """Retrieves the data from an order report."""
        url = ASpApiBaseURL.IN.value+'/reports/2021-06-30/documents/' + document_id

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers)
        result = response.json()

        # Dump the response headers
        logger.info('*' * 150)
        logger.info('Amazon SP Client retrieve_report, Response Headers:')
        response_headers = {}
        for header, value in response.headers.items():
            response_headers[header] = value
            logger.info(f'{header}: {value}')

        # Dump the response status and response code
        response_code = response.status_code
        logger.info(f'Response Status: {response.status_code}')
        logger.info(f'Response Code: {response.status_code}')

        rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
        logger.info(rate_limit)

        """A 429 is a retry-able status code, Read the x-amzn-RateLimit-Limit header, when available."""
        if response_code == 429 and rate_limit is None:
            logger.info('Rate limit is None. Retrying in 300 seconds...')
            time.sleep(300)
            return self.retrieve_report(document_id)

        return result


    def get_scheduled_report(self, params):

        url = ASpApiBaseURL.IN.value + '/reports/2021-06-30/reports'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers, params=params)
        result = response.json()

        return result

    def get_rdt_token(self, payload):

        url = ASpApiBaseURL.IN.value + '/tokens/2021-03-01/restrictedDataToken'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('POST', url, auth=aws_auth, headers=headers, json=payload)
        result = response.json()

        return result

    def get_orders(self, params):

        url = ASpApiBaseURL.IN.value + '/orders/v0/orders'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers, params=params)
        result = response.json()

        return result

    def get_sales(self, params, web_call=False):

        url = ASpApiBaseURL.IN.value + '/sales/v1/orderMetrics'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers, params=params)

        if not web_call:
            result = response.json()

            logger.info('*' * 150)
            logger.info('Amazon SP Client get_sales:')
            logger.info(f'Url: {url}')
            logger.info(f'Params: {params}')
            logger.info('Amazon SP Client get_sales, Response Headers:')
            response_headers = {}
            for header, value in response.headers.items():
                response_headers[header] = value
                logger.info(f'{header}: {value}')

            # Dump the response status and response code
            response_code = response.status_code
            logger.info(f'Response Status: {response.status_code}')
            logger.info(f'Response Code: {response.status_code}')

            rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
            logger.info(rate_limit)

            """A 429 is a retry-able status code, Read the x-amzn-RateLimit-Limit header, when available."""
            if response_code == 429 and rate_limit is None:
                logger.info('Rate limit is None. Retrying in 300 seconds...')
                time.sleep(300)
                return self.get_sales(params=params)

            return result

        return response

    def get_fba_inventory(self, params):

        url = ASpApiBaseURL.IN.value + '/fba/inventory/v1/summaries'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        logger.info(f'params {params}')

        response = requests.request('GET', url, auth=aws_auth, headers=headers, params=params)

        result = response.json()

        # Dump the response headers
        logger.info('*' * 150)
        logger.info(f'FBA Url: {url}')
        logger.info('Params: \n{_payload}'.format(_payload=params))
        logger.info('Amazon SP Client get_fba_inventory, Response Headers:')
        response_headers = {}
        for header, value in response.headers.items():
            response_headers[header] = value
            logger.info(f'{header}: {value}')

        # Dump the response status and response code
        response_code = response.status_code
        logger.info(f'Response Status: {response.status_code}')
        logger.info(f'Response Code: {response.status_code}')

        rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
        logger.info(rate_limit)

        """A 429 is a retry-able status code, Read the x-amzn-RateLimit-Limit header, when available."""
        if response_code == 429 and rate_limit is None:
            logger.info('Rate limit is None. Retrying in 300 seconds...')
            time.sleep(300)
            return self.get_fba_inventory(params=params)

        return result

    def get_next_page_result(self, params):

        url = ASpApiBaseURL.IN.value + '/orders/v0/orders/'

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers, params=params)

        result = response.json()
        return result

    def get_order_items(self, order_id):

        url = ASpApiBaseURL.IN.value + '/orders/v0/orders/{}/orderItems'.format(order_id)

        aws_auth = generate_seller_api_awssigv4()

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, auth=aws_auth, headers=headers)
        result = response.json()

        return result


    def get_my_fees_estimate_for_sku(self, payload, seller_sku):
        seller_sku = quote_plus(seller_sku)

        url = ASpApiBaseURL.IN.value + f'/products/fees/v0/listings/{seller_sku}/feesEstimate'

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
            'Content-Type': 'application/json'
        }

        aws_auth = generate_seller_api_awssigv4()

        response = requests.request('POST', url, auth=aws_auth, headers=headers, data=payload)

        result = response.json()

        return result

    def get_fees_estimates(self, payload, asin):
        asin = quote_plus(asin)

        url = ASpApiBaseURL.IN.value + f'/products/fees/v0/items/{asin}/feesEstimate'

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
            'Content-Type': 'application/json'
        }

        aws_auth = generate_seller_api_awssigv4()

        response = requests.request('POST', url, auth=aws_auth, headers=headers, data=payload)

        result = response.json()

        return result

    def get_financial_events(self, params, _response_headers=False, _rate_limit=False, _response_status=False):
        """Get's Financial event's report data."""

        url = ASpApiBaseURL.IN.value+'/finances/v0/financialEvents'

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        logger.info('*' * 100)
        logger.info('Request Body:')
        logger.info(params)
        logger.info('Request Headers:')
        logger.info(headers)

        response = requests.request('GET', url, headers=headers, params=params)
        result = response.json()

        # Dump the response headers
        logger.info('*' * 100)
        logger.info('Response Headers:')
        response_headers = {}
        for header, value in response.headers.items():
            response_headers[header] = value
            logger.info(f'{header}: {value}')

        # Dump the response status and response code
        response_status_code = response.status_code
        logger.info(f'Response Status: {response.status_code}')
        logger.info(f'Response Code: {response.status_code}')

        rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
        logger.info(rate_limit)

        if _response_headers or _rate_limit or _response_status:
            return result, response_headers, rate_limit, response_status_code

        return result

    def get_catalog_items(self, params):
        """Retrieves details for an item in the Amazon catalog."""
        url = ASpApiBaseURL.IN.value + '/catalog/2022-04-01/items'

        headers = {
            'host': ASpApiBaseURL.IN.value[8:],
            'x-amz-access-token': self.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.request('GET', url, headers=headers, params=params)

        result = response.json()

        # Dump the response headers
        logger.info('*' * 150)
        logger.info('Amazon SP Client get_catalog_items, Response Headers:')
        response_headers = {}
        for header, value in response.headers.items():
            response_headers[header] = value
            logger.info(f'{header}: {value}')

        # Dump the response status and response code
        response_code = response.status_code
        logger.info(f'Response Status: {response.status_code}')
        logger.info(f'Response Code: {response.status_code}')

        rate_limit = response.headers.get('x-amzn-RateLimit-Limit', None)
        logger.info(rate_limit)

        """A 429 is a retry-able status code, Read the x-amzn-RateLimit-Limit header, when available."""
        if response_code == 429 and rate_limit is None:
            logger.info('Rate limit is None. Retrying in 300 seconds...')
            time.sleep(300)
            return self.get_catalog_items(params=params)

        return result
