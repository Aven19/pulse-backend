"""All the constants which is used in project is defined in this file."""
import enum
import re
import sys
from typing import Any

APP_NAME = 'EcommPulse'
PAGE_DEFAULT = 1
PAGE_LIMIT = 10
PRODUCT_RANK_LIMIT = 20

EXCEL_ALLOWED_EXTENSIONS = ['xls', 'xlsx']

FBA_RETURNS_MAX_REFUND_CLAIM_DAYS = 75
FINANCIAL_EVENTS_MAX_RESULTS_PER_PAGE = 30


def str_to_class(classname):
    """Use getattr to retrieve the class object by name from the current module"""

    return getattr(sys.modules[__name__], classname)


class EnumBase(enum.Enum):
    """Base class for all enums with common method"""
    @classmethod
    def get_name(cls, status):
        """Returns the name of each item in enum"""
        for name, member in cls.__members__.items():
            if member.value == status:
                return str(member.value)
        return None

    @classmethod
    def validate_name(cls, status: Any) -> Any:
        """This method returns key name of enum from value."""
        if status:
            for name, member in cls.__members__.items():
                if member.name.lower() == status.lower():
                    return name
        return None


class Methods(enum.Enum):
    """ http methods used"""
    GET = 'GET'
    POST = 'POST'
    PATCH = 'PATCH'
    DELETE = 'DELETE'


class CognitoProviderType(enum.Enum):
    """ Cognito Identity Provider types"""
    GOOGLE = 'Google'
    LOGIN_WITH_AMZON = 'LoginWithAmazon'


class ResponseMessageKeys(enum.Enum):
    """API response messages for various purposes"""
    ERROR_IS_ON_US = 'Error is on us.'
    MISSING_TOKEN = 'Missing Token.'
    INVALID_TOKEN = 'Invalid Token.'
    TOKEN_EXPIRED = 'Token Expired, Try sign in again.'
    ENTER_CORRECT_INPUT = 'Enter correct input.'
    PLEASE_TRY_AFTER_SECONDS = 'Please try after{0}s.'
    PROJECT_ALREADY_CLOSED = 'Closed Project cannot be updated.'
    TOKEN_VALID = 'Token verified successfully.'
    SUCCESS = 'Details Fetched Successfully.'
    NO_DATA_FOUND = 'No data found.'
    ADDED = 'Added Successfully.'
    UPDATED = 'Details updated Successfully'
    DELETED = 'Deleted Successfully'
    FAILED = 'Something went wrong.'
    SELLER_NOT_EXIST = 'Seller not found'
    FILE_UPLOAD_SUCCESS = 'File uploaded successfully'
    IMPORT_QUEUED = 'File uploaded successfully, please check reports for status.'
    EXPORT_QUEUED = 'Your request is accepted, please check reports for status.'
    SKU_DOES_NOT_EXIST = 'SKU does not exist'
    USER_ADDED_SUCCESSFULLY = 'User added successfully.'
    USER_DETAILS_FETCHED_SUCCESSFULLY = 'User details fetched successfully.'
    USER_ALREADY_EXIST = 'User already exists'
    USER_PASS_RESET_REQUIRED = 'Password Reset is required'
    USER_NOT_CONFIRMED = 'Please Change Temporary Password.'
    USER_CONFIRMED = 'User already accepted the invitation.'
    LOGIN_SUCCESSFULLY = 'Hi, great to see you!'
    LOGIN_FAILED = 'Login Failed.'
    INVALID_PASSWORD = 'Invalid password.'
    FAILED_EMAIL_VALIDATION = 'Invalid email.'
    USER_NOT_EXIST = 'Entered Email ID is not registered with us.'
    USER_DETAILS_NOT_FOUND = 'User details not found.'
    USER_ACCOUNT_NOT_LINKED = 'User not linked to an account'
    ACCESS_DENIED = 'Access denied'
    ACCOUNT_EXISTS = 'Account details already mapped'
    GOOGLE_AUTH_URI = 'Google login URI generated successfully'
    AMAZON_AUTH_URI = 'Amazon login URI generated successfully'
    LOGOUT = 'Logout URI generated successfully'
    ASP_URI = 'Amazon seller partner URI generated successfully'
    FSP_URI = 'Flipkart seller partner URI generated successfully'
    FSP_CONNECTED = 'Flipkart seller partner connected successfully'
    INVALID_ACCOUNT = 'Invalid account token'
    ASP_CONNECTED = 'Amazon seller partner connected successfully'
    ASP_SYNC_SUCCESS = 'Amazon seller partner API synced successfully'
    ASP_SYNC_RUNNING = 'Sit back while we sync up your data. It might take up to 48 hours.'
    AZ_AD_SYNC_SUCCESS = 'Amazon Ads API synced successfully'
    ASP_TOKEN_EXPIRED = 'Token not found or Expired, Try sign in again.'
    ASP_INVALID_TOKEN = 'Token Expired for seller partner, Try sign in again.'
    ADS_URI = 'Amazon Ads URI generated successfully'
    ADS_API_CONNECTED = "Ad's api connected successfully"
    ADS_INFO_SAVED = "Ad's Information saved sucessfully"
    ADS_INVALID_TOKEN = "Token Expired for Amazon Ad's, Try sign in again."
    USER_INVITE_SUCCESSFUL = 'User invite has been sent successfully'
    USER_ALREADY_INVITED = 'User has been invited already'
    USER_INVITE_PENDING = 'User has not accepted the invite yet'
    USER_INVITE_RESENT = 'Invitation has been resent'
    USER_ACCEPTED_INVITATION = 'User has accepted the sent invitation'
    USER_REJECTED_INVITATION = 'User has rejected the sent invitation'
    USER_INVITE_DOES_NOT_EXIST = 'Invite does not exist'
    USER_INVITE_LINK_EXPIRED = 'The invite link you are trying to use has expired. Please request a new invitation.'
    USER_INVITE_VALID = 'The invite link is valid.'
    SUBSCRIPTION_INACTIVE = 'The subscription is inactive or expired'
    SUBSCRIPTION_LINK_CREATED = 'Subscription link created'
    SUBSCRIPTION_ORDER_CREATED = 'Subscription and order for payment has been created'
    SUBSCRIPTION_CANCELLED = 'Your subscription is cancelled successfully'
    SUBSCRIPTION_ALREADY_ACTIVE = 'Your account already has an active subscription'
    PAYMENT_SIGNATURE_INVALID = 'Payment signature invalid'
    INFO_SAVED = 'Details saved sucessfully'
    INVALID_BRAND = 'Brand selected is invalid for this request'
    ACCOUNT_DEACTIVATED = 'Account deactivated successfully'
    ACCOUNT_ACTIVATED = 'Account activated successfully'
    ACCOUNT_ALREADY_EXISTS = 'Account already exists'
    ACCOUNT_DOES_NOT_EXIST = 'Account does not exist'
    INACTIVE_USER_ACCOUNT = 'User is inactive for the account'
    USER_ACTIVATED = 'User activated successfully'
    USER_DEACTIVATED = 'User deactivated'
    ADS_PROFILE_EXISTS = 'Cannot map account details, as it is already mapped with another account.'


class EmailSubject(enum.Enum):
    """Emails subject's texts"""
    WELCOME_TO_PULSE = "You're invited as a Ecomm Pulse Admin!"


class ValidationMessages(enum.Enum):
    """ Validation messages for different fields."""
    UUID = 'Enter valid UUID.'


class Preference(enum.Enum):
    """User Preference types"""
    EMAIL_NOTIFICATIONS = 'EMAIL_NOTIFICATIONS'
    MOBILE_NOTIFICATIONS = 'MOBILE_NOTIFICATIONS'
    DATE_FORMAT = 'DATE_FORMAT'
    TIME_FORMAT = 'TIME_FORMAT'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.MOBILE_NOTIFICATIONS.value:
            return 'Mobile Notifications'
        elif status == cls.EMAIL_NOTIFICATIONS.value:
            return 'Email Notifications'
        elif status == cls.DATE_FORMAT.value:
            return 'Date Format'
        elif status == cls.TIME_FORMAT.value:
            return 'Time Format'
        else:
            return 'N/A'


class QueueName:
    """redis queue scheduler names"""
    EXPORT_CSV = 'EXPORT_CSV'
    EXPORT_EXCEL = 'EXPORT_EXCEL'
    ITEM_MASTER_COGS_IMPORT = 'ITEM_MASTER_COGS_IMPORT'

    ITEM_MASTER_REPORT = 'ITEM_MASTER_REPORT'
    ORDER_REPORT = 'ORDER_REPORT'
    SALES_TRAFFIC_REPORT = 'SALES_TRAFFIC_REPORT'
    LEDGER_SUMMARY_REPORT = 'LEDGER_SUMMARY_REPORT'
    FBA_RETURNS_REPORT = 'FBA_RETURNS_REPORT'
    FBA_REIMBURSEMENTS_REPORT = 'FBA_REIMBURSEMENTS_REPORT'
    FBA_CUSTOMER_SHIPMENT_SALES_REPORT = 'FBA_CUSTOMER_SHIPMENT_SALES_REPORT'

    SETTLEMENT_REPORT_V2 = 'SETTLEMENT_REPORT_V2'

    ASP_CREATE_REPORT = 'ASP_CREATE_REPORT'
    ASP_VERIFY_REPORT = 'ASP_VERIFY_REPORT'

    AZ_ADS_CREATE_REPORT = 'AZ_ADS_CREATE_REPORT'
    AZ_ADS_VERIFY_REPORT = 'AZ_ADS_VERIFY_REPORT'

    AZ_SPONSORED_BRAND = 'AZ_SPONSORED_BRAND'
    AZ_SPONSORED_DISPLAY = 'AZ_SPONSORED_DISPLAY'
    AZ_SPONSORED_PRODUCT = 'AZ_SPONSORED_PRODUCT'

    ITEM_MASTER_UPDATE_CATALOG = 'ITEM_MASTER_UPDATE_CATALOG'
    FINANCE_EVENT_LIST = 'FINANCE_EVENT_LIST'

    AZ_PERFORMANCE_ZONE = 'AZ_PERFORMANCE_ZONE'

    SES_EMAIL_DELIVERY = 'SES_EMAIL_DELIVERY'

    SUBSCRIPTION_CHECK = 'SUBSCRIPTION_CHECK'

    REDIS = 'REDIS'

    SALES_ORDER_METRICS = 'SALES_ORDER_METRICS'

    FBA_INVENTORY = 'FBA_INVENTORY'


class QueueTaskStatus(enum.Enum):

    """ Queue Task Status """
    NEW = 10
    RUNNING = 20
    ERROR = 30
    COMPLETED = 40

    @classmethod
    def get_status(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.NEW.value:
            return 'New'
        if status == cls.RUNNING.value:
            return 'Running'
        if status == cls.ERROR.value:
            return 'Error'
        if status == cls.COMPLETED.value:
            return 'Completed'
        else:
            return 'N/A'


class ReportTaskStatus(enum.Enum):
    """Report task status enum."""
    RUNNING = 10
    ERROR = 20
    COMPLETED = 30

    @classmethod
    def get_status(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.RUNNING.value:
            return 'Running'
        if status == cls.ERROR.value:
            return 'Error'
        if status == cls.COMPLETED.value:
            return 'Completed'
        else:
            return 'N/A'


class DateFormats(EnumBase):
    """Ecomm Pulse selectable date format preferences for app."""
    DDMMYYYY = 'DD/MM/YYYY'
    DDMMMYYYY = 'DD/MMM/YYYY'
    MMDDYYYY = 'MM/DD/YYYY'
    MMMDDYYYY = 'MMM/DD/YYYY'

    @classmethod
    def get_python_date_format(cls, date_format):
        """Returns the python date format syntax for the passed date format"""
        if date_format == cls.DDMMYYYY.value:
            return '%d/%m/%Y'
        elif date_format == cls.DDMMMYYYY.value:
            return '%d/%b/%Y'
        elif date_format == cls.MMDDYYYY.value:
            return '%m/%d/%Y'
        elif date_format == cls.MMMDDYYYY.value:
            return '%b/%d/%Y'
        else:
            return '%d/%b/%Y'


class TimeFormats(EnumBase):
    """Ecomm Pulse selectable time format preferences for app."""
    HHMM12 = 'HH:MM 12 Hour'
    HHMMSS12 = 'HH:MM:SS 12 Hour'
    HHMM24 = 'HH:MM 24 Hour'
    HHMMSS24 = 'HH:MM:SS 24 Hour'

    @classmethod
    def get_python_time_format(cls, time_format):
        """Returns the python time format syntax for the passed time format"""
        if time_format == cls.HHMM12.value:
            return '%I:%M %p'
        elif time_format == cls.HHMM24.value:
            return '%H:%M'
        elif time_format == cls.HHMMSS12.value:
            return '%I:%M:%S %p'
        elif time_format == cls.HHMMSS24.value:
            return '%H:%M:%S'
        else:
            return '%I:%M %p'


class NotificationStatus(enum.Enum):
    """Notification status types"""
    OPENED = 'OPENED'
    ERROR = 'ERROR'


class TimePeriod(EnumBase):
    """Report durations or periods"""
    LAST_3_DAYS = 'Last 3 Days'
    LAST_7_DAYS = 'Last 7 Days'
    LAST_30_DAYS = 'Last 30 Days'
    LAST_MONTH = 'Last Month'
    CURRENT_MONTH = 'Current Month'
    LAST_YEAR = 'Last Year'
    CUSTOM = 'Custom'


class DeviceTypes(EnumBase):
    """Enum for storing type of different devices."""
    ANDROID = 'Android'
    IOS = 'iOS'


class ReportTask:
    """Enum for storing report type."""
    USER_LIST_EXPORT = 'Export User List'
    ORGANIZATION_LIST_EXPORT = 'Export Organization List'
    CREW_MEMBER_LIST_EXPORT = 'Export Crew Member List'
    EXPENSE_CATEGORY_LIST_EXPORT = 'Expense Category List'


class InputValidation:
    """regex validations for various input/field types"""
    STRING = '^[a-zA-Z]+$'
    STRING_SPACE = '^[a-zA-Z\s]+$'
    STRING_UPPERCASE = '^[A-Z]+$'
    STRING_UPPERCASE_SPACE = '^[A-Z\s]+$'
    STRING_LOWERCASE_SPACE = '^[a-z\s]+$'
    NUMERIC = '^[1-9][0-9]*$'
    ALPHA_NUMERIC = '^[A-Za-z0-9]+$'
    ALPHA_NUMERIC_SPACE = '^[A-Za-z0-9\s]+$'
    MOBILE_NUMBER = '^[0-9]{0,16}$'
    PASSWORD = '^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^_&*-]).{8,}$'
    EMAIL = '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    ALPHA_NUMERIC_SPACE_DASH = '^[-A-Za-z0-9\s]+$'
    EXP_CAT_NAME_LEN = 20
    PROJECT_MAX_NAME_LEN = 50
    MAX_DESCRIPTION_LEN = 140

    @classmethod
    def is_valid(cls, string, validation_type):
        """This method validated Regular Expression."""
        return re.match(pattern=validation_type, string=str(string))


class CurrencyType(EnumBase):
    """Currencies supported by Ecomm Pulse."""
    GBP = 'GBP'
    EUR = 'EUR'
    USD = 'USD'

    @classmethod
    def get_symbol(cls, currency):
        """This method returns symbol for each type of currency."""
        if currency == cls.GBP.value:
            return '£'
        elif currency == cls.USD.value:
            return '$'
        elif currency == cls.EUR.value:
            return '€'
        else:
            return 'N/A'


class UserType(enum.Enum):
    """Ecomm Pulse user types."""
    ADMIN = 'ADMIN'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.ADMIN.value:
            return 'Admin'
        else:
            return 'N/A'


class VerificationStatus(enum.Enum):
    """Enum for different verification status."""
    UNVERIFIED = 'UNVERIFIED'
    VERIFIED = 'VERIFIED'
    EXVERIFIED = 'EXVERIFIED'
    REFERRED = 'REFERRED'
    DECLINED = 'DECLINED'
    REVIEWED = 'REVIEWED'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.UNVERIFIED.value:
            return 'Verification incomplete'
        elif status == cls.VERIFIED.value:
            return 'Verification complete'
        elif status == cls.EXVERIFIED.value:
            return ' Verified externally'
        elif status == cls.REFERRED.value:
            return 'Pending manual review'
        elif status == cls.DECLINED.value:
            return 'Verification failed'
        elif status == cls.REVIEWED.value:
            return 'Verification check reviewed'
        else:
            return 'N/A'


class AccountStatus(EnumBase):
    """Enum for account status."""
    ACTIVATE = 'ACTIVATE'
    DEACTIVATE = 'DEACTIVATE'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.ACTIVATE.value:
            return 'ACTIVATE'
        elif status == cls.DEACTIVATE.value:
            return 'DEACTIVATE'
        else:
            return 'N/A'


class UserStatus(enum.Enum):
    """Enum for user status."""
    ACTIVE = 'ACTIVE'
    DEACTIVATED = 'DEACTIVATED'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.ACTIVE.value:
            return 'Active'
        elif status == cls.DEACTIVATED.value:
            return 'Deactivated'


class ChangeUserStatus(EnumBase):
    """Enum for changing user status"""

    ACTIVATE = 'ACTIVATE'
    DEACTIVATE = 'DEACTIVATE'


class SecQ(enum.Enum):
    """Enum for security questions."""
    FIRST_PET_NAME = 'FIRST_PET_NAME'
    MATERNAL_GRANDMOTHER_MAIDEN_NAME = 'MATERNAL_GRANDMOTHER_MAIDEN_NAME'
    FAVOURITE_CHILDHOOD_FRIEND = 'FAVOURITE_CHILDHOOD_FRIEND'
    FIRST_CAR = 'FIRST_CAR'
    CITY_PARENTS_MET = 'CITY_PARENTS_MET'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.FIRST_PET_NAME.value:
            return "What was my first pet's name?"
        elif status == cls.MATERNAL_GRANDMOTHER_MAIDEN_NAME.value:
            return "What was my maternal grandmother's maiden name?"
        elif status == cls.FAVOURITE_CHILDHOOD_FRIEND.value:
            return 'Who was my favourite childhood friend?'
        elif status == cls.FIRST_CAR.value:
            return 'What was my first car?'
        elif status == cls.CITY_PARENTS_MET.value:
            return 'Where did my parents first meet?'
        else:
            return 'N/A'


class RedisCacheKeys(EnumBase):
    """Enum for storing redis keys for caching."""
    AMAZON_ACCESS_TOKEN = 'AMAZON_ACCESS_TOKEN'
    ADS_PERFORMANCE_BY_ZONE = 'ADS_PERFORMANCE_BY_ZONE'


class TimeInSeconds(EnumBase):
    """Enum that will return time in seconds from different keys. (ex.Five_MIN=300)"""
    FIVE_MIN = 300
    THIRTY_MIN = 1800
    SIXTY_MIN = 3600
    TWENTY_FOUR_HOUR = 86400
    ONE_MONTH = 2592000


class ApiMethods(enum.Enum):
    """Enum for storing API methods."""
    POST = 'ACTIVE'
    GET = 'BLOCKED'
    PUT = 'CREATED'
    DELETE = 'SUSPENDED'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.POST.value:
            return 'Post'
        elif status == cls.GET.value:
            return 'Get'
        elif status == cls.PUT.value:
            return 'Put'
        elif status == cls.DELETE.value:
            return 'Delete'
        else:
            return 'N/A'


class EmailTypes(enum.Enum):
    """Enum for storing different Email Types."""
    INVITE = 'invite'
    SET_PIN = 'set_pin'
    KYB_LINK = 'kyb_link'
    RESET_ADMIN_PASSWORD = 'reset_admin_password'
    OVERDUE_REMINDER = 'overdue_reminder'
    APPROVAL_REMINDER = 'approval_reminder'
    CREW_REMOVED_DEACTIVATED = 'crew_removed_deactivated'


class AttachmentType(enum.Enum):
    """Enum for storing different Attachment Types."""
    ITEM_MASTER_COGS_UPLOAD = 10
    ITEM_MASTER_EXPORT = 20
    SETTLEMENT_V2_CSV_EXPORT = 30
    PRODUCT_PERFORMANCE_EXPORT = 40
    ADS_PERFORMANCE_BY_ZONE_EXPORT = 50
    MR_PRODUCT_PERFORMANCE_EXPORT = 60
    ACCOUNT_PROFILE_PICTURE = 70

    @classmethod
    def get_type(cls, status):
        """This method returns key name of enum from value."""
        if status == 10:
            return cls.ITEM_MASTER_EXPORT.value
        elif status == 70:
            return cls.PRODUCT_PERFORMANCE_EXPORT.value
        elif status == 120:
            return cls.ADS_PERFORMANCE_BY_ZONE_EXPORT.value
        elif status == 160:
            return cls.MR_PRODUCT_PERFORMANCE_EXPORT.value
        elif status == 200:
            return cls.ACCOUNT_PROFILE_PICTURE.value
        else:
            return None


class EntityType(enum.Enum):
    """Enum for storing entity type."""
    ITEM_MASTER = 10
    ORDER_REPORT = 20
    SETTLEMENT_V2_REPORT = 30
    SALES_TRAFFIC_REPORT = 40
    FINANCE_EVENT_LIST = 50
    LEDGER_SUMMARY_REPORT = 60
    PRODUCT_PERFORMANCE = 70
    AZ_SPONSORED_BRAND_BANNER = 80
    AZ_SPONSORED_BRAND_VIDEO = 90
    AZ_SPONSORED_DISPLAY = 100
    AZ_SPONSORED_PRODUCT = 110
    ADS_PERFORMANCE_BY_ZONE = 120
    FBA_RETURNS_REPORT = 130
    FBA_REIMBURSEMENTS_REPORT = 140
    FBA_CUSTOMER_SHIPMENT_SALES_REPORT = 150
    MR_PRODUCT_PERFORMANCE = 160
    MR_PERFORMANCE_ZONE = 170
    SES_EMAIL_DELIVERY = 180
    SUBSCRIPTION_CHECK = 190
    ACCOUNT_PROFILE_PICTURE = 200
    SALES_ORDER_METRICS = 210
    FBA_INVENTORY = 220

    @classmethod
    def get_type(cls, status):                                          # type: ignore  # noqa: C901
        """This method returns key name of enum from value."""
        if status == cls.ITEM_MASTER.value:
            return 'Item Master'
        if status == cls.ORDER_REPORT.value:
            return 'Order Report'
        if status == cls.SETTLEMENT_V2_REPORT.value:
            return 'Settlement V2 Report'
        if status == cls.SALES_TRAFFIC_REPORT.value:
            return 'Sales and Traffic Report'
        if status == cls.FINANCE_EVENT_LIST.value:
            return 'Finance Event List'
        if status == cls.LEDGER_SUMMARY_REPORT.value:
            return 'Ledger Summary Report'
        if status == cls.AZ_SPONSORED_BRAND_BANNER.value:
            return 'Sponsored Brand Banner Report'
        if status == cls.AZ_SPONSORED_BRAND_VIDEO.value:
            return 'Sponsored Brand Video Report'
        if status == cls.AZ_SPONSORED_DISPLAY.value:
            return 'Sponsored Display Report'
        if status == cls.AZ_SPONSORED_PRODUCT.value:
            return 'Sponsored Product Report'
        if status == cls.PRODUCT_PERFORMANCE.value:
            return 'Product Performance'
        if status == cls.ADS_PERFORMANCE_BY_ZONE.value:
            return 'Performance By Zone'
        if status == cls.FBA_RETURNS_REPORT.value:
            return 'FBA Concessions Report, Returns'
        if status == cls.FBA_REIMBURSEMENTS_REPORT.value:
            return 'FBA Payments Report, Reimbursements'
        if status == cls.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value:
            return 'FBA Sales Report, Customer Shipment'
        if status == cls.MR_PRODUCT_PERFORMANCE.value:
            return 'Marketing Report Product Performance Export'
        if status == cls.MR_PERFORMANCE_ZONE.value:
            return 'Marketing Report Performance Zone'
        if status == cls.SES_EMAIL_DELIVERY.value:
            return 'SES email delivery'
        if status == cls.ACCOUNT_PROFILE_PICTURE.value:
            return 'Account Profile'
        if status == cls.SALES_ORDER_METRICS.value:
            return 'Sales Order Metrics'
        if status == cls.FBA_INVENTORY.value:
            return 'FBA Inventory API'
        else:
            return 'N/A'


class SubEntityType(enum.Enum):
    """Enum for sub entity type."""
    ICON = 10
    BANNER = 20
    FACEIMAGE = 30
    EXPORT_FILE = 40
    IMAGE = 50


class HttpStatusCode(enum.Enum):
    """Enum for storing different http status cocde."""
    OK = '200'
    BAD_REQUEST = '400'
    UNAUTHORIZED = '401'
    FORBIDDEN = '403'
    NOT_FOUND = '404'
    INTERNAL_SERVER_ERROR = '500'
    TOO_MANY_REQUESTS = '429'
    NO_CONTENT = '204'
    GONE = '410'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.OK.value:
            return 200
        elif status == cls.BAD_REQUEST.value:
            return 400
        elif status == cls.UNAUTHORIZED.value:
            return 401
        elif status == cls.FORBIDDEN.value:
            return 403
        elif status == cls.INTERNAL_SERVER_ERROR.value:
            return 500
        elif status == cls.NO_CONTENT.value:
            return 204
        elif status == cls.GONE.value:
            return 410
        else:
            return None


class SortingOrder(EnumBase):
    """Enum for storing sorting parameters value."""
    ASC = 'asc'
    DESC = 'desc'


class SortingParams(EnumBase):
    """Enum for storing transactions."""
    AMOUNT = 'AMOUNT'
    DATE = 'DATE'
    NAME = 'NAME'
    TITLE = 'TITLE'


class ASpReportProcessingStatus(enum.Enum):
    """Enum for different Report Type."""

    # The report was cancelled. There are two ways a report can be cancelled: an explicit cancellation request before the report starts processing, or an automatic cancellation if there is no data to return.
    CANCELLED = 'CANCELLED'
    # The report has completed processing and a reportDocumentId is available.
    DONE = 'DONE'
    SUCCESS = 'SUCCESS'

    FATAL = 'FATAL'  # The report was stopped due to a fatal error and a reportDocumentId may be present. If present, the report represented by the reportDocumentId may explain why the report processing ended.
    # The report is being processed.
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    PROCESSING = 'PROCESSING'
    # The report has not yet started processing. It may be waiting for another IN_PROGRESS report.
    IN_QUEUE = 'IN_QUEUE'

    # Ecomm Pulse Status
    NEW = 'NEW'
    ERROR = 'ERROR'
    FETCHED = 'FETCHED'
    COMPLETED = 'COMPLETED'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class AspRestrictedReports(enum.Enum):
    """Enum for different Report Type."""

    AMAZON_FULFILLED_SHIPMENTS_DATA_INVOICING = 'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_INVOICING'
    AMAZON_FULFILLED_SHIPMENTS_DATA_TAX = 'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_TAX'
    FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING = 'GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING'
    FLAT_FILE_ORDER_REPORT_DATA_SHIPPING = 'GET_FLAT_FILE_ORDER_REPORT_DATA_SHIPPING'
    FLAT_FILE_ORDER_REPORT_DATA_INVOICING = 'GET_FLAT_FILE_ORDER_REPORT_DATA_INVOICING'
    FLAT_FILE_ORDER_REPORT_DATA_TAX = 'GET_FLAT_FILE_ORDER_REPORT_DATA_TAX'
    FLAT_FILE_ORDERS_RECONCILIATION_DATA_TAX = 'GET_FLAT_FILE_ORDERS_RECONCILIATION_DATA_TAX'
    FLAT_FILE_ORDERS_RECONCILIATION_DATA_INVOICING = 'GET_FLAT_FILE_ORDERS_RECONCILIATION_DATA_INVOICING'
    FLAT_FILE_ORDERS_RECONCILIATION_DATA_SHIPPING = 'GET_FLAT_FILE_ORDERS_RECONCILIATION_DATA_SHIPPING'
    ORDER_REPORT_DATA_INVOICING = 'GET_ORDER_REPORT_DATA_INVOICING'
    ORDER_REPORT_DATA_TAX = 'GET_ORDER_REPORT_DATA_TAX'
    ORDER_REPORT_DATA_SHIPPING = 'GET_ORDER_REPORT_DATA_SHIPPING'
    EASYSHIP_DOCUMENTS = 'GET_EASYSHIP_DOCUMENTS'
    TAX_REPORT_GST_MTR_B2B = 'GET_GST_MTR_B2B_CUSTOM'
    VAT_TRANSACTION_DATA = 'GET_VAT_TRANSACTION_DATA'
    SC_VAT_TAX_REPORT = 'SC_VAT_TAX_REPORT'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class ASpReportType(enum.Enum):
    """Enum for different Report Type."""

    ITEM_MASTER_LIST_ALL_DATA = 'GET_MERCHANT_LISTINGS_ALL_DATA'
    LEDGER_SUMMARY_VIEW_DATA = 'GET_LEDGER_SUMMARY_VIEW_DATA'
    SALES_TRAFFIC_REPORT = 'GET_SALES_AND_TRAFFIC_REPORT'
    FBA_RETURNS_REPORT = 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA'
    FBA_REIMBURSEMENTS_REPORT = 'GET_FBA_REIMBURSEMENTS_DATA'
    FBA_CUSTOMER_SHIPMENT_SALES_REPORT = 'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA'
    ORDER_REPORT_ORDER_DATE_GENERAL = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

    SETTLEMENT_REPORT_FLAT_FILE_V2 = 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2'
    SETTLEMENT_REPORT_DATA_FLAT = 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE'
    TAX_REPORT_GST_MTR_B2B = 'GET_GST_MTR_B2B_CUSTOM'
    TAX_REPORT_GST_MTR_B2C = 'GET_GST_MTR_B2C_CUSTOM'
    TAX_REPORT_B2B_STR_ADHOC = 'GET_GST_STR_ADHOC'
    ORDER_REPORT_DATA_INVOICING = 'GET_ORDER_REPORT_DATA_INVOICING'
    FLAT_FILE_ORDER_REPORT_DATA_INVOICING = 'GET_FLAT_FILE_ORDER_REPORT_DATA_INVOICING'
    FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING = 'GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING'
    FBA_REPLACEMENTS_REPORT = 'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA'
    FBA_ESTIMATED_FBA_FEES_TXT_DATA = 'GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA'
    RETURN_REPORT_RETURN_DATE = 'GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE'
    RETURN_REPORT_CSV_MFN_PRIME = 'GET_CSV_MFN_PRIME_RETURNS_REPORT'
    RETURN_REPORT_FLAT_FILE_ATTRIBUTES = 'GET_FLAT_FILE_MFN_SKU_RETURN_ATTRIBUTES_REPORT'

    SALES_ORDER_METRICS = 'SALES_ORDER_METRICS'
    FBA_INVENTORY = 'FBA_INVENTORY'

    AZ_SPONSORED_BRAND_BANNER = 'GET_SPONSORED_BRAND_BANNER_V2'
    AZ_SPONSORED_BRAND_VIDEO = 'GET_SPONSORED_BRAND_VIDEO_V2'
    AZ_SPONSORED_DISPLAY = 'GET_SPONSORED_DISPLAY_V2'
    AZ_SPONSORED_PRODUCT = 'GET_SPONSORED_PRODUCT_V3'

    # Ecomm-pulse defined report
    LIST_FINANCIAL_EVENTS = 'LIST_FINANCIAL_EVENTS'
    GET_CATALOG_ITEM = 'GET_CATALOG_ITEM'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class ASpApiBaseURL(enum.Enum):

    """Enum for Amazon Seller Partner Base Url"""

    # BASE_URL = 'https://sandbox.sellingpartnerapi'
    BASE_URL = 'https://sellingpartnerapi'

    AE = f'{BASE_URL}-eu.amazon.com'
    BE = f'{BASE_URL}-eu.amazon.com'
    DE = f'{BASE_URL}-eu.amazon.com'
    PL = f'{BASE_URL}-eu.amazon.com'
    EG = f'{BASE_URL}-eu.amazon.com'
    ES = f'{BASE_URL}-eu.amazon.com'
    FR = f'{BASE_URL}-eu.amazon.com'
    GB = f'{BASE_URL}-eu.amazon.com'
    IN = f'{BASE_URL}-eu.amazon.com'  # Endpoint for India
    IT = f'{BASE_URL}-eu.amazon.com'
    NL = f'{BASE_URL}-eu.amazon.com'
    SA = f'{BASE_URL}-eu.amazon.com'
    SE = f'{BASE_URL}-eu.amazon.com'
    TR = f'{BASE_URL}-eu.amazon.com'
    UK = f'{BASE_URL}-eu.amazon.com'
    AU = f'{BASE_URL}-fe.amazon.com'
    JP = f'{BASE_URL}-fe.amazon.com'
    SG = f'{BASE_URL}-fe.amazon.com'
    US = f'{BASE_URL}-na.amazon.com'
    BR = f'{BASE_URL}-na.amazon.com'
    CA = f'{BASE_URL}-na.amazon.com'
    MX = f'{BASE_URL}-na.amazon.com'

    @classmethod
    def get_name(cls, status):
        """Get the base URL for a specific country code (status)."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class ASpMarketplace(enum.Enum):

    """Enum for Amazon Seller Marketplace"""

    NORTH_AMERICA = 'NORTH_AMERICA'
    EUROPE = 'EUROPE'
    FAR_EAST = 'FAR_EAST'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.NORTH_AMERICA.value:
            return 'North America'
        elif status == cls.EUROPE.value:
            return 'Europe'
        elif status == cls.FAR_EAST.value:
            return 'Far East'
        return None


class ASpMarketplaceId(enum.Enum):

    """Enum for Amazon Seller Marketplace Id"""

    EU = 'A1RKKUPIHCS9HS'
    NA = 'ATVPDKIKX0DER'
    BR = 'A2Q3Y263D00KWC'
    ES = 'A1RKKUPIHCS9HS'
    UK = 'A1F83G8C2ARO7P'
    FR = 'A13V1IB3VIYZZH'
    BE = 'AMEN7PMS3EDWL'
    NL = 'A1805IZSGTT6HS'
    DE = 'A1PA6795UKMFR9'
    IT = 'APJ6JRA9NG5V4'
    SE = 'A2NODRKZP88ZB9'
    ZA = 'AE08WJ6YKNBMC'
    PL = 'A1C3SOZRARQ6R3'
    EG = 'ARBP9OOSHTCHU'
    TR = 'A33AVAJ2PDY3EV'
    SA = 'A17E79C6D8DWNP'
    AE = 'A2VIGQ35RCS4UG'
    IN = 'A21TJRUUN4KGV'  # Market Place Id for India
    SG = 'A19VAU5U5O7RUS'
    AU = 'A39IBJ37TRP1C6'
    JP = 'A1VC38T7YXB528'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class ASpURL(enum.Enum):
    """Enum for API URL's."""

    AMAZON_API_BASE_URL = 'https://api.amazon.com'
    # Front end callback URL
    AUTHORIZE_CONFIRM_CALLBACK_URL = '/api/v1/account/connect/amazon/callback/front-end'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class FulfillmentChannel(EnumBase):
    """Enum for fullfillment channel."""
    AMAZON_IN = 'AMAZON_IN'
    DEFAULT = 'DEFAULT'


class OrderFulfillmentChannel(enum.Enum):
    """Enum for orders fullfillment channel."""
    MFN = 'MFN'
    AFN = 'AFN'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class ItemMasterStatus(EnumBase):
    """Enum for item master status."""
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'
    INCOMPLETE = 'INCOMPLETE'


class DateGranularity(enum.Enum):
    """Enum for date granularity"""

    DAY = 'DAY'
    WEEK = 'WEEK'
    MONTH = 'MONTH'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class AsinGranularity(enum.Enum):
    """Enum for ASIN granularity"""

    PARENT = 'PARENT'
    CHILD = 'CHILD'
    SKU = 'SKU'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class AggregateByLocation(enum.Enum):
    """Enum for aggregate location"""

    FC = 'FC'
    COUNTRY = 'COUNTRY'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class AggregateByTimePeriod(enum.Enum):
    """Enum for aggregate time period"""

    DAILY = 'DAILY'
    MONTHLY = 'MONTHLY'
    WEEKLY = 'WEEKLY'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class SalesChannel(enum.Enum):
    """Enum for sales channel in order report"""

    AMAZON_IN = 'Amazon.in'
    DEFAULT = 'default'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class OpenUrl(EnumBase):

    """ Endpoints to exclude authentication"""
    URLS = ['/api/v1/user/register', '/api/v1/user/authenticate', '/api/v1/user/auth/google',
            '/api/v1/user/auth/amazon', '/api/v1/account/connect/amazon', '/api/v1/account/connect/amazon/callback/front-end', '/api/v1/auth/amazon/lwa-refresh-token', '/api/v1/auth/flipkart',
            '/api/v1/user/auth/idp/callback/front-end', '/api/v1/user/auth/idp/callback', '/api/v1/user/auth/idp/logout',
            '/api/v1/user/auth/google', '/api/v1/user/auth/amazon', '/api/v1/account/connect/amazon/ads-api', '/api/v1/account/connect/amazon/ads-api/callback/front-end']


class ProductMarketplace(EnumBase):
    """Enum for product marketplace"""

    AMAZON = 'AMAZON'
    FLIPKART = 'FLIPKART'
    SHOPIFY = 'SHOPIFY'
    ALL_MARKETPLACE = 'ALL_MARKETPLACE'


class AdsApiURL(enum.Enum):
    """Enum for ADs api urls"""
    BASE_URL = 'https://advertising-api-eu.amazon.com'
    SPONSORED_BRAND_V2 = f'{BASE_URL}/v2/hsa/keywords/report'
    SPONSORED_BRAND_V3 = f'{BASE_URL}/reporting/reports'
    SPONSORED_DISPLAY_V2 = f'{BASE_URL}/sd/productAds/report'

    @classmethod
    def get_name(cls, status):
        """Get the base URL for a specific Ad"""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class AspFinanceEventList(enum.Enum):
    """Enum for different Report Type."""

    SHIPMENT = 'ShipmentEventList'
    REFUND = 'RefundEventList'
    SERVICE_FEE = 'ServiceFeeEventList'
    PRODUCT_ADS_PAYMENT = 'ProductAdsPaymentEventList'
    ADJUSTMENT = 'AdjustmentEventList'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class AspFinanceType(enum.Enum):
    """Enum for different Report Type."""

    PRODUCT_ADS_PAYMENT = 'PRODUCT_ADS_PAYMENT'
    CS_ERROR_ITEMS = 'CS_ERROR_ITEMS'
    REVERSAL_REIMBURSEMENT = 'REVERSAL_REIMBURSEMENT'
    WAREHOUSE_DAMAGE = 'WAREHOUSE_DAMAGE'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class DbAnomalies(enum.Enum):
    """Enum for different DB Anomalies."""

    INSERTION = 'INSERTION'
    DELETION = 'DELETION'
    UPDATE = 'UPDATE'

    @classmethod
    def get_name(cls, status):
        """This method returns key name of enum from value."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class SponsoredBrandCreativeType(enum.Enum):
    """Enum for sponsored brand creative Type."""

    ALL = 'all'
    VIDEO = 'video'


class AzSponsoredAdMetrics(enum.Enum):
    """Enum for sponsored ads metrics."""

    # SB - SPONSORED BRAND
    SB_ALL_METRICS = 'adGroupId,adGroupName,applicableBudgetRuleId,applicableBudgetRuleName,attributedConversions14d,attributedConversions14dSameSKU,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSales14d,attributedSales14dSameSKU,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignRuleBasedBudget,campaignStatus,clicks,cost,dpv14d,impressions,keywordBid,keywordId,keywordStatus,keywordText,matchType,searchTermImpressionRank,searchTermImpressionShare,unitsSold14d,attributedBrandedSearches14d,topOfSearchImpressionShare'
    SB_VIDEO_METRICS = 'adGroupId,adGroupName,attributedConversions14d,attributedConversions14dSameSKU,attributedSales14d,attributedSales14dSameSKU,campaignBudget,campaignBudgetType,campaignId,campaignName,campaignStatus,clicks,cost,impressions,keywordBid,keywordId,keywordStatus,keywordText,matchType,vctr,video5SecondViewRate,video5SecondViews,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,viewableImpressions,vtr,dpv14d,attributedDetailPageViewsClicks14d,attributedOrderRateNewToBrand14d,attributedOrdersNewToBrand14d,attributedOrdersNewToBrandPercentage14d,attributedSalesNewToBrand14d,attributedSalesNewToBrandPercentage14d,attributedUnitsOrderedNewToBrand14d,attributedUnitsOrderedNewToBrandPercentage14d,attributedBrandedSearches14d,currency,topOfSearchImpressionShare'

    # SD - SPONSORED DISPLAY
    SD_METRICS = 'adGroupId,adGroupName,adId,asin,attributedConversions14d,attributedConversions14dSameSKU,attributedConversions1d,attributedConversions1dSameSKU,attributedConversions30d,attributedConversions30dSameSKU,attributedConversions7d,attributedConversions7dSameSKU,attributedDetailPageView14d,attributedOrdersNewToBrand14d,attributedSales14d,attributedSales14dSameSKU,attributedSales1d,attributedSales1dSameSKU,attributedSales30d,attributedSales30dSameSKU,attributedSales7d,attributedSales7dSameSKU,attributedSalesNewToBrand14d,attributedUnitsOrdered14d,attributedUnitsOrdered1d,attributedUnitsOrdered30d,attributedUnitsOrdered7d,attributedUnitsOrderedNewToBrand14d,campaignId,campaignName,clicks,cost,currency,impressions,sku,viewAttributedConversions14d,viewImpressions,viewAttributedDetailPageView14d,viewAttributedSales14d,viewAttributedUnitsOrdered14d,viewAttributedOrdersNewToBrand14d,viewAttributedSalesNewToBrand14d,viewAttributedUnitsOrderedNewToBrand14d,attributedBrandedSearches14d,viewAttributedBrandedSearches14d,videoCompleteViews,videoFirstQuartileViews,videoMidpointViews,videoThirdQuartileViews,videoUnmutes,vtr,vctr,avgImpressionsFrequency,cumulativeReach'

    # SP - SPONSORED PRODUCT
    SP_COLUMNS = ['startDate', 'endDate', 'campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'adId', 'portfolioId', 'impressions', 'clicks', 'costPerClick', 'clickThroughRate', 'cost', 'spend', 'campaignBudgetCurrencyCode', 'campaignBudgetAmount', 'campaignBudgetType', 'campaignStatus', 'advertisedAsin', 'advertisedSku', 'purchases1d', 'purchases7d', 'purchases14d', 'purchases30d', 'purchasesSameSku1d', 'purchasesSameSku7d', 'purchasesSameSku14d', 'purchasesSameSku30d', 'unitsSoldClicks1d',
                  'unitsSoldClicks7d', 'unitsSoldClicks14d', 'unitsSoldClicks30d', 'sales1d', 'sales7d', 'sales14d', 'sales30d', 'attributedSalesSameSku1d', 'attributedSalesSameSku7d', 'attributedSalesSameSku14d', 'attributedSalesSameSku30d', 'salesOtherSku7d', 'unitsSoldSameSku1d', 'unitsSoldSameSku7d', 'unitsSoldSameSku14d', 'unitsSoldSameSku30d', 'unitsSoldOtherSku7d', 'kindleEditionNormalizedPagesRead14d', 'kindleEditionNormalizedPagesRoyalties14d', 'acosClicks7d', 'acosClicks14d', 'roasClicks7d', 'roasClicks14d']


class AzSponsoredAdPayloadData(enum.Enum):
    """Enum for sponsored ads payload details."""

    # SD - SPONSORED DISPLAY
    SD_TACTIC = 'T00020'

    # SP - SPONSORED PRODUCT
    SP_AD_PRODUCT = 'SPONSORED_PRODUCTS'
    SP_GROUP_BY = ['advertiser']
    SP_REPORT_TYPE_ID = 'spAdvertisedProduct'
    SP_TIME_UNIT = 'SUMMARY'
    SP_FORMAT = 'GZIP_JSON'
    SP_NAME = 'SP advertised product report'


class MarketingReportZones(EnumBase):
    """Enum for different zones in marketing report"""

    OPTIMAL_ZONE = 'OPTIMAL_ZONE'
    OPPORTUNITY_ZONE = 'OPPORTUNITY_ZONE'
    WORK_IN_PROGRESS_ZONE = 'WORK_IN_PROGRESS_ZONE'


class AzProductPerformanceColumn(EnumBase):
    """Enum for Product Performance Column"""

    GROSS_SALES = 'GROSS_SALES'
    UNITS_SOLD = 'UNITS_SOLD'
    REFUNDS = 'REFUNDS'
    UNITS_RETURNED = 'UNITS_RETURNED'
    RETURN_RATE = 'RETURN_RATE'
    MARKETPLACE_FEE = 'MARKETPLACE_FEE'
    PRODUCT_COST = 'COGS'
    PROFIT = 'PROFIT'
    ASP = 'ASP'
    MARGIN = 'MARGIN'
    TOTAL_ORDERS = 'UNIQUE_ORDERS'

    @classmethod
    def get_column(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.GROSS_SALES.value:
            return 'total_gross_sales'
        if status == cls.UNITS_SOLD.value:
            return 'total_units_sold'
        if status == cls.REFUNDS.value:
            return 'total_refunds'
        if status == cls.UNITS_RETURNED.value:
            return 'total_units_returned'
        if status == cls.RETURN_RATE.value:
            return 'returns_rate'
        if status == cls.MARKETPLACE_FEE.value:
            return 'market_place_fee'
        if status == cls.PRODUCT_COST.value:
            return 'cogs'
        if status == cls.PROFIT.value:
            return 'profit'
        if status == cls.ASP.value:
            return 'average_selling_price'
        if status == cls.MARGIN.value:
            return 'margin'
        if status == cls.TOTAL_ORDERS.value:
            return 'unique_orders'
        else:
            return None


class AzSalesByRegionColumn(EnumBase):
    """Enum for Sales by Region Column"""

    GROSS_SALES = 'GROSS_SALES'
    REFUNDS = 'REFUNDS'

    @classmethod
    def get_column(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.GROSS_SALES.value:
            return 'total_gross_sales'
        if status == cls.REFUNDS.value:
            return 'total_refunds'
        else:
            return None


class AzRefundInsightsColumn(EnumBase):
    """Enum for Sales by Region Column"""

    REFUND_DATE = 'REFUND_DATE'

    @classmethod
    def get_column(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.REFUND_DATE.value:
            return 'report_date'
        else:
            return None


class AzInventoryLevelColumn(EnumBase):
    """Enum for Sales by Region Column"""

    PRICE = 'PRICE'
    PRODUCT_COST = 'PRODUCT_COST'
    SELLABLE_QUANTITY = 'SELLABLE_QUANTITY'
    UNFULFILLABLE_QUANTITY = 'UNFULFILLABLE_QUANTITY'
    IN_TRANSIT_QUANTITY = 'IN_TRANSIT_QUANTITY'
    AVG_DAILY_SALES_30 = 'AVG_DAILY_SALES_30'
    DAYS_OF_INVENTORY = 'DAYS_OF_INVENTORY'
    IN_STOCK_RATE = 'IN_STOCK_RATE'
    VALUE_OF_STOCK = 'VALUE_OF_STOCK'
    TOTAL_QUANTITY = 'TOTAL_QUANTITY'

    @classmethod
    def get_column(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.PRICE.value:
            return 'price'
        if status == cls.PRODUCT_COST.value:
            return 'product_cost'
        if status == cls.SELLABLE_QUANTITY.value:
            return 'sellable_quantity'
        if status == cls.UNFULFILLABLE_QUANTITY.value:
            return 'unfulfillable_quantity'
        if status == cls.IN_TRANSIT_QUANTITY.value:
            return 'in_transit_quantity'
        if status == cls.AVG_DAILY_SALES_30.value:
            return 'avg_daily_sales_30_days'
        if status == cls.DAYS_OF_INVENTORY.value:
            return 'days_of_inventory'
        if status == cls.IN_STOCK_RATE.value:
            return 'in_stock_rate'
        if status == cls.VALUE_OF_STOCK.value:
            return 'value_of_stock'
        if status == cls.TOTAL_QUANTITY.value:
            return 'total_quantity'
        else:
            return None


class AzFbaReturnsReportType(EnumBase):
    """Enum for Fba Returns Report type"""

    CARRIER_DAMAGED = 'CARRIER_DAMAGED'
    DEFECTIVE = 'DEFECTIVE'
    SELLABLE = 'SELLABLE'
    CUSTOMER_DAMAGED = 'CUSTOMER_DAMAGED'
    DAMAGED = 'DAMAGED'
    LOST = 'LOST'


class AzPaApiOperations(enum.Enum):

    """
    Get the target key name for a specific enum value.

    Args:
        status (str): The enum value for which to retrieve the target key name.

    Returns:
        str: The target key name associated with the provided enum value, or None if not found.
    """

    GET_ITEMS = 'GetItems'

    @classmethod
    def get_target(cls, status):
        """This method returns key name of enum from value."""
        if status == cls.GET_ITEMS.value:
            return 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems'


class AzPaApiResources(EnumBase):
    """Enum for types of resources in Product advertising api"""

    # BROWSENODEINFO_BROWSENODES = "BrowseNodeInfo.BrowseNodes"
    BROWSENODEINFO_BROWSENODES_ANCESTOR = 'BrowseNodeInfo.BrowseNodes.Ancestor'
    BROWSENODEINFO_BROWSENODES_SALESRANK = 'BrowseNodeInfo.BrowseNodes.SalesRank'
    BROWSENODEINFO_WEBSITESALESRANK = 'BrowseNodeInfo.WebsiteSalesRank'
    # CUSTOMERREVIEWS_COUNT = "CustomerReviews.Count"
    # CUSTOMERREVIEWS_STARRATING = "CustomerReviews.StarRating"
    IMAGES_PRIMARY_SMALL = 'Images.Primary.Small'
    IMAGES_PRIMARY_MEDIUM = 'Images.Primary.Medium'
    IMAGES_PRIMARY_LARGE = 'Images.Primary.Large'
    IMAGES_PRIMARY_HIGHRES = 'Images.Primary.HighRes'
    IMAGES_VARIANTS_SMALL = 'Images.Variants.Small'
    IMAGES_VARIANTS_MEDIUM = 'Images.Variants.Medium'
    IMAGES_VARIANTS_LARGE = 'Images.Variants.Large'
    IMAGES_VARIANTS_HIGHRES = 'Images.Variants.HighRes'
    # ITEMINFO_BYLINEINFO = "ItemInfo.ByLineInfo"
    # ITEMINFO_CONTENTINFO = "ItemInfo.ContentInfo"
    # ITEMINFO_CONTENTRATING = "ItemInfo.ContentRating"
    # ITEMINFO_CLASSIFICATIONS = "ItemInfo.Classifications"
    # ITEMINFO_EXTERNALIDS = "ItemInfo.ExternalIds"
    # ITEMINFO_FEATURES = "ItemInfo.Features"
    # ITEMINFO_MANUFACTUREINFO = "ItemInfo.ManufactureInfo"
    # ITEMINFO_PRODUCTINFO = "ItemInfo.ProducftInfo"
    # ITEMINFO_TECHNICALINFO = "ItemInfo.TechnicalInfo"
    # ITEMINFO_TITLE = "ItemInfo.Title"
    # ITEMINFO_TRADEININFO = "ItemInfo.TradeInInfo"
    # OFFERS_LISTINGS_AVAILABILITY_MAXORDERQUANTITY = "Offers.Listings.Availability.MaxOrderQuantity"
    # OFFERS_LISTINGS_AVAILABILITY_MESSAGE = "Offers.Listings.Availability.Message"
    # OFFERS_LISTINGS_AVAILABILITY_MINORDERQUANTITY = "Offers.Listings.Availability.MinOrderQuantity"
    # OFFERS_LISTINGS_AVAILABILITY_TYPE = "Offers.Listings.Availability.Type"
    # OFFERS_LISTINGS_CONDITION = "Offers.Listings.Condition"
    # OFFERS_LISTINGS_CONDITION_CONDITIONNOTE = "Offers.Listings.Condition.ConditionNote"
    # OFFERS_LISTINGS_CONDITION_SUBCONDITION = "Offers.Listings.Condition.SubCondition"
    OFFERS_LISTINGS_DELIVERYINFO_ISAMAZONFULFILLED = 'Offers.Listings.DeliveryInfo.IsAmazonFulfilled'
    # OFFERS_LISTINGS_DELIVERYINFO_ISFREESHIPPINGELIGIBLE = "Offers.Listings.DeliveryInfo.IsFreeShippingEligible"
    OFFERS_LISTINGS_DELIVERYINFO_ISPRIMEELIGIBLE = 'Offers.Listings.DeliveryInfo.IsPrimeEligible'
    # OFFERS_LISTINGS_DELIVERYINFO_SHIPPINGCHARGES = "Offers.Listings.DeliveryInfo.ShippingCharges"
    # OFFERS_LISTINGS_ISBUYBOXWINNER = "Offers.Listings.IsBuyBoxWinner"
    # OFFERS_LISTINGS_LOYALTYPOINTS_POINTS = "Offers.Listings.LoyaltyPoints.Points"
    OFFERS_LISTINGS_MERCHANTINFO = 'Offers.Listings.MerchantInfo'
    OFFERS_LISTINGS_PRICE = 'Offers.Listings.Price'
    # OFFERS_LISTINGS_PROGRAMELIGIBILITY_ISPRIMEEXCLUSIVE = "Offers.Listings.ProgramEligibility.IsPrimeExclusive"
    # OFFERS_LISTINGS_PROGRAMELIGIBILITY_ISPRIMEPANTRY = "Offers.Listings.ProgramEligibility.IsPrimePantry"
    # OFFERS_LISTINGS_PROMOTIONS = "Offers.Listings.Promotions"
    # OFFERS_LISTINGS_SAVINGBASIS = "Offers.Listings.SavingBasis"
    # OFFERS_SUMMARIES_HIGHESTPRICE = "Offers.Summaries.HighestPrice"
    # OFFERS_SUMMARIES_LOWESTPRICE = "Offers.Summaries.LowestPrice"
    OFFERS_SUMMARIES_OFFERCOUNT = 'Offers.Summaries.OfferCount'
    # PARENTASIN = "ParentASIN"
    # RENTALOFFERS_LISTINGS_AVAILABILITY_MAXORDERQUANTITY = "RentalOffers.Listings.Availability.MaxOrderQuantity"
    # RENTALOFFERS_LISTINGS_AVAILABILITY_MESSAGE = "RentalOffers.Listings.Availability.Message"
    # RENTALOFFERS_LISTINGS_AVAILABILITY_MINORDERQUANTITY = "RentalOffers.Listings.Availability.MinOrderQuantity"
    # RENTALOFFERS_LISTINGS_AVAILABILITY_TYPE = "RentalOffers.Listings.Availability.Type"
    # RENTALOFFERS_LISTINGS_BASEPRICE = "RentalOffers.Listings.BasePrice"
    # RENTALOFFERS_LISTINGS_CONDITION = "RentalOffers.Listings.Condition"
    # RENTALOFFERS_LISTINGS_CONDITION_CONDITIONNOTE = "RentalOffers.Listings.Condition.ConditionNote"
    # RENTALOFFERS_LISTINGS_CONDITION_SUBCONDITION = "RentalOffers.Listings.Condition.SubCondition"
    # RENTALOFFERS_LISTINGS_DELIVERYINFO_ISAMAZONFULFILLED = "RentalOffers.Listings.DeliveryInfo.IsAmazonFulfilled"
    # RENTALOFFERS_LISTINGS_DELIVERYINFO_ISFREESHIPPINGELIGIBLE = "RentalOffers.Listings.DeliveryInfo.IsFreeShippingEligible"
    # RENTALOFFERS_LISTINGS_DELIVERYINFO_ISPRIMEELIGIBLE = "RentalOffers.Listings.DeliveryInfo.IsPrimeEligible"
    # RENTALOFFERS_LISTINGS_DELIVERYINFO_SHIPPINGCHARGES = "RentalOffers.Listings.DeliveryInfo.ShippingCharges"
    # RENTALOFFERS_LISTINGS_MERCHANTINFO = "RentalOffers.Listings.MerchantInfo"


class AzPaApiBaseURL(enum.Enum):

    """Enum for Amazon Seller Partner Base Url"""

    BASE_URL = 'https://webservices.amazon'

    IN = f'{BASE_URL}.in'  # Endpoint for India

    @classmethod
    def get_name(cls, status):
        """ Get the base URL for a specific country code (status)."""
        if status in cls.__members__:
            return cls.__members__[status].value
        else:
            return 'N/A'


class CalculationLevelEnum(enum.Enum):
    """Enum for level at which calculation should happen"""

    # when no filter is applied, the calculations are at account level
    ACCOUNT = 'ACCOUNT'

    # when any filter is applied , the calculations are at product level
    PRODUCT = 'PRODUCT'


class UserInviteStatus(enum.Enum):

    """Enum for user invite status"""

    PENDING = 'PENDING'
    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'


class PlanInterval(enum.Enum):

    """Enum for plan interval interval"""

    MONTHLY = 1
    DAILY = 7
    YEARLY = 1

    @classmethod
    def get_interval(cls, status):
        """This method returns key name of enum from value."""
        if status.upper() in cls.__members__:
            return cls.__members__[status.upper()].value
        else:
            return 'N/A'


class BillingInterval(enum.Enum):

    """Enum for plan interval interval"""
    """Weekly only for ecomm-trail"""

    # billing will not repeat after 7 week/ 12 month / 52 days / 1 year
    # WEEKLY = 7
    # MONTHLY = 12
    # DAILY = 52
    # YEARLY = 1

    # billing will not repeat after 1 week/ 1 month / 7 days / 1 year
    WEEKLY = 1
    MONTHLY = 1
    DAILY = 7
    YEARLY = 1

    @classmethod
    def get_interval(cls, status):
        """This method returns key name of enum from value."""
        if status.upper() in cls.__members__:
            return cls.__members__[status.upper()].value
        else:
            return 'N/A'


class WebhookEvent(enum.Enum):
    """Enum for razorpay webhook entity"""

    # order events
    ORDER_PAID = 'order.paid'

    # payment events
    PAYMENT_AUTHORIZED = 'payment.authorized'
    PAYMENT_CAPTURED = 'payment.captured'
    PAYMENT_FAILED = 'payment.failed'

    # invoice events
    INVOICE_PARTIALLY_PAID = 'invoice.partially_paid'
    INVOICE_PAID = 'invoice.paid'
    INVOICE_EXPIRED = 'invoice.expired'

    # Subscription events
    SUBSCRIPTION_AUTHENTICATED = 'subscription.authenticated'
    SUBSCRIPTION_ACTIVATED = 'subscription.activated'
    SUBSCRIPTION_CHARGED = 'subscription.charged'
    SUBSCRIPTION_COMPLETED = 'subscription.completed'
    SUBSCRIPTION_UPDATED = 'subscription.updated'
    SUBSCRIPTION_PENDING = 'subscription.pending'
    SUBSCRIPTION_HALTED = 'subscription.halted'
    SUBSCRIPTION_CANCELLED = 'subscription.cancelled'
    SUBSCRIPTION_PAUSED = 'subscription.paused'
    SUBSCRIPTION_RESUMED = 'subscription.resumed'


class SubscriptionStates(enum.Enum):
    """Enum for subscription states from razorpay"""

    AUTHENTICATED = 'authenticated'
    PENDING = 'pending'
    HALTED = 'halted'
    ACTIVE = 'active'
    EXPIRED = 'expired'
    INACTIVE = 'inactive'
    # Subscription created status
    CREATED = 'created'


class EcommPulsePlanName(EnumBase):

    """ Endpoints to exclude authentication"""
    ECOMM_FREE_TRAIL = 'ECOMM_FREE_TRAIL'


class SubscriptionOpenUrl(EnumBase):

    """ Endpoints to exclude authentication"""
    URLS = ['/api/v1/user/account/list',
            '/api/v1/billing-and-plans/get-plans', '/api/v1/payment/create-subscription', '/api/v1/payment/get-all-susbcriptions', '/api/v1/payment/verify-payment-checkout',
            '/api/v1/account/connect/amazon',
            '/api/v1/account/connect/amazon/callback',
            '/api/v1/account/connect/amazon/sp-profile-info/store',
            '/api/v1/account/connect/amazon/ads-api/callback',
            '/api/v1/account/connect/amazon/ads-profile-info/store',
            '/api/v1/account/connect/amazon/ads-api',
            '/api/v1/profile/get', '/api/v1/profile/add-update']


class SalesAPIGranularity(EnumBase):
    """Enum for sales api granularity"""

    HOUR = 'Hour'
    DAY = 'Day'
    WEEK = 'Week'
    MONTH = 'Month'
    YEAR = 'Year'
    TOTAL = 'Total'
