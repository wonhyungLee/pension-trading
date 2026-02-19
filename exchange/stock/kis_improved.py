import time
import httpx
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import traceback
import copy
from dataclasses import dataclass
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# 기존 POA 모듈들 import
try:
    from exchange.stock.error import TokenExpired
    from exchange.stock.schemas import *
    from exchange.database import db
    from pydantic import validate_arguments
    from exchange.model import MarketOrder
    from exchange.utility import log_message
    from exchange.utility.kis_debugger import KISDebugger, debug_kis_method
except ImportError:
    print("Warning: POA modules not found, using mock implementations")
    class TokenExpired(Exception): pass
    class MarketOrder: pass
    def log_message(msg): print(f"LOG: {msg}")
    class db:
        @staticmethod
        def set_auth(auth_id, token, expires): pass
        @staticmethod
        def get_auth(auth_id): return None
        @staticmethod
        def delete_auth(auth_id): pass
    class KISDebugger:
        def __init__(self, *args, **kwargs): 
            import logging
            self.logger = logging.getLogger("mock_logger")
        def log_api_call(self, *args, **kwargs): pass
        def log_auth_event(self, *args, **kwargs): pass
        def debug_token_validity(self, *args, **kwargs): return {}
        def generate_debug_report(self, *args, **kwargs): return ""
    def debug_kis_method(debugger):
        def decorator(func):
            return func
        return decorator

@dataclass
class AssetInfo:
    """자산 정보 데이터 클래스"""
    symbol: str
    name: str
    quantity: float
    average_price: float
    current_price: float
    eval_amount: float      # 원화 평가액
    eval_amount_usd: float  # 달러 평가액
    market_type: str

class ImprovedKoreaInvestment:
    def __init__(self, key: str, secret: str, account_number: str, 
                 account_code: str, kis_number: int):
        self.key = key
        self.secret = secret
        self.account_number = account_number
        self.account_code = account_code
        self.kis_number = kis_number
        
        # 모의투자 여부 확인 (settings.py 또는 appkey 'V' 포함 여부)
        try:
            from exchange.utility.setting import settings
            # KIS{n}_MOCK_TRADE 와 같은 설정이 있는지 확인
            is_mock_setting = getattr(settings, f'KIS{kis_number}_MOCK_TRADE', 'V' in self.key)
        except (ImportError, AttributeError):
            is_mock_setting = 'V' in self.key

        if is_mock_setting:
            self.base_url = "https://openapivts.koreainvestment.com:29443"
        else:
            self.base_url = "https://openapi.koreainvestment.com:9443"
        
        try:
            self.debugger = KISDebugger(kis_number, is_mock=is_mock_setting)
            self.debug_method = debug_kis_method(self.debugger)
        except (NameError, TypeError):
            print(f"Warning: KISDebugger not found or is a mock for KIS{kis_number}")
            self.debugger = None
            self.debug_method = lambda func: func
        
        self.access_token = None
        self.token_expires_at = None
        self.last_auth_check = None
        self.is_auth = False
        self.base_headers = {}

        # 데이터베이스에서 기존 토큰 정보 로드
        try:
            auth_id = f"KIS{self.kis_number}"
            auth_data = db.get_auth(auth_id)
            if auth_data:
                token, expires_str = auth_data
                expires_dt = datetime.strptime(expires_str, "%Y-%m-%d %H:%M:%S")
                if datetime.now() + timedelta(minutes=10) < expires_dt:
                    self.access_token = token
                    self.token_expires_at = expires_dt
                    self.is_auth = True
                    self.base_headers = {
                        "authorization": f"Bearer {self.access_token}",
                        "appkey": self.key,
                        "appsecret": self.secret,
                        "custtype": "P"
                    }
                    if self.debugger: self.debugger.log_auth_event("token_loaded_from_db", True, {"expires_at": expires_str})
        except Exception as e:
            if self.debugger: self.debugger.log_auth_event("token_db_load_error", False, {"error": str(e)})
        
        self.api_call_times = []
        self.max_calls_per_second = 18
        self.max_calls_per_minute = 950
        
        self.session = httpx.Client(timeout=30.0)
        
        self.base_order_body = AccountInfo(
            CANO=account_number, ACNT_PRDT_CD=account_code
        )
        
        self.order_exchange_code = {
            "NASDAQ": ExchangeCode.NASDAQ,
            "NYSE": ExchangeCode.NYSE,
            "AMEX": ExchangeCode.AMEX,
        }
        self.query_exchange_code = {
            "NASDAQ": QueryExchangeCode.NASDAQ,
            "NYSE": QueryExchangeCode.NYSE,
            "AMEX": QueryExchangeCode.AMEX,
        }

        self._ensure_authentication()

    def init_info(self, order_info: MarketOrder):
        self.order_info = order_info
    
    def _rate_limit_check(self):
        now = time.time()
        recent_calls_sec = [t for t in self.api_call_times if now - t < 1.0]
        if len(recent_calls_sec) >= self.max_calls_per_second:
            time.sleep(1.1 - (now - min(recent_calls_sec)))
        
        recent_calls_min = [t for t in self.api_call_times if now - t < 60.0]
        if len(recent_calls_min) >= self.max_calls_per_minute:
            time.sleep(61.0 - (now - min(recent_calls_min)))

        self.api_call_times = [t for t in recent_calls_min if now - t < 60.0]
        self.api_call_times.append(time.time())
    
    def _ensure_authentication(self) -> bool:
        if (not self.access_token or not self.token_expires_at or
                datetime.now() + timedelta(minutes=10) >= self.token_expires_at):
            return self._refresh_token()
        if (not self.last_auth_check or 
                datetime.now() - self.last_auth_check > timedelta(minutes=5)):
            return self._validate_token()
        return True
    
    def _refresh_token(self) -> bool:
        if self.debugger: self.debugger.log_auth_event("token_refresh_start", True)
        endpoint = "/oauth2/tokenP"
        data = {"grant_type": "client_credentials", "appkey": self.key, "appsecret": self.secret}
        try:
            response = self.session.post(f"{self.base_url}{endpoint}", json=data)
            response.raise_for_status()
            res_data = response.json()
            if "access_token" in res_data:
                self.access_token = res_data["access_token"]
                expires_str = res_data["access_token_token_expired"]
                self.token_expires_at = datetime.strptime(expires_str, "%Y-%m-%d %H:%M:%S")
                self.last_auth_check = datetime.now()
                self.is_auth = True
                self.base_headers = {"authorization": f"Bearer {self.access_token}", "appkey": self.key, "appsecret": self.secret, "custtype": "P"}
                if self.debugger: self.debugger.log_auth_event("token_refresh_success", True, {"expires_at": expires_str})
                try: db.set_auth(f"KIS{self.kis_number}", self.access_token, expires_str)
                except: pass
                return True
            else:
                if self.debugger: self.debugger.log_auth_event("token_refresh_failed", False, res_data)
                return False
        except Exception as e:
            if self.debugger: self.debugger.log_auth_event("token_refresh_error", False, {"error": str(e)})
            return False

    def _validate_token(self) -> bool:
        try:
            endpoint = "/uapi/domestic-stock/v1/quotations/inquire-ccnl"
            headers = self._get_headers("FHKST01010300")
            params = { "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "005930" }
            self._rate_limit_check()
            response = self.session.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
            res_data = response.json()
            if res_data.get("msg_cd") == "EGW00123":
                if self.debugger: self.debugger.log_auth_event("token_validation_expired", False)
                return self._refresh_token()
            self.last_auth_check = datetime.now()
            if self.debugger: self.debugger.log_auth_event("token_validation_success", True)
            return True
        except Exception as e:
            if self.debugger: self.debugger.log_auth_event("token_validation_error", False, {"error": str(e)})
            return False

    def _get_headers(self, tr_id: str) -> Dict[str, str]:
        return {"Content-Type": "application/json; charset=utf-8", "authorization": f"Bearer {self.access_token}", "appkey": self.key, "appsecret": self.secret, "tr_id": tr_id, "custtype": "P"}

    def _clear_cached_auth(self) -> None:
        try:
            db.delete_auth(f"KIS{self.kis_number}")
        except Exception as e:
            if self.debugger:
                self.debugger.logger.warning(f"Failed to clear cached auth: {e}")

    def _recreate_session(self) -> None:
        try:
            if hasattr(self, "session") and self.session:
                self.session.close()
        except Exception:
            pass
        self.session = httpx.Client(timeout=30.0)
        self.api_call_times = []

    def _hard_reconnect(self, clear_auth_cache: bool) -> bool:
        if self.debugger:
            self.debugger.logger.warning(
                f"Hard reconnect requested (clear_auth_cache={clear_auth_cache}) for KIS{self.kis_number}"
            )

        self._recreate_session()
        self.last_auth_check = None

        if clear_auth_cache:
            self._clear_cached_auth()
            self.access_token = None
            self.token_expires_at = None
            self.is_auth = False
            self.base_headers = {}
            return self._refresh_token()

        return self._ensure_authentication()

    def _is_connection_error(self, error: Exception) -> bool:
        if isinstance(error, (httpx.TransportError, httpx.TimeoutException, json.JSONDecodeError)):
            return True
        msg = str(error).lower()
        return any(
            key in msg
            for key in (
                "server disconnected",
                "connection refused",
                "connection reset",
                "timed out",
                "temporarily unavailable",
                "remote protocol error",
                "client has been closed",
                "cannot send a request",
            )
        )

    def _is_auth_error(self, error: Exception) -> bool:
        msg = str(error).lower()
        return any(
            key in msg
            for key in (
                "authentication failed",
                "token refresh failed",
                "invalid token",
                "access token",
                "egw00123",
                "접근토큰",
                "토큰",
                "appkey",
                "appsecret",
            )
        )

    def _api_call_with_retry(self, method: str, endpoint: str, headers: Dict, params: Dict = None, data: Dict = None, max_retries: int = 3) -> Dict:
        for attempt in range(max_retries):
            try:
                if not self._ensure_authentication(): raise Exception("Authentication failed")
                headers["authorization"] = f"Bearer {self.access_token}"
                self._rate_limit_check()
                
                if method.upper() == "GET":
                    response = self.session.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
                else:
                    response = self.session.post(f"{self.base_url}{endpoint}", headers=headers, json=data)
                
                try:
                    res_data = response.json()
                except json.JSONDecodeError:
                    print(f"CRITICAL: KIS API JSON Decode Error. Status={response.status_code}, URL={response.url}, Response='{response.text}'")
                    raise

                if self.debugger: self.debugger.log_api_call(endpoint, method, headers, params or data, res_data)
                
                if res_data.get("rt_cd") == "0": return res_data
                elif res_data.get("msg_cd") == "EGW00123":
                    if self.debugger: self.debugger.logger.warning(f"Token expired, refreshing...")
                    if self._refresh_token(): continue
                    else: raise Exception("Token refresh failed")
                elif str(res_data.get("msg_cd", "")).startswith("EGW") and "토큰" in str(res_data.get("msg1", "")):
                    if self.debugger:
                        self.debugger.logger.warning("Auth-related API error received; clearing cache and reconnecting")
                    self._hard_reconnect(clear_auth_cache=True)
                    continue
                else:
                    raise Exception(f"API error: {res_data.get('msg1', 'Unknown error')}")
            except Exception as e:
                if self.debugger: self.debugger.logger.error(f"Attempt {attempt + 1} failed: {e}")
                if self._is_connection_error(e) or self._is_auth_error(e):
                    # On connection/auth failures, force reconnect and optionally clear cached auth.
                    # For repeated failures, clear auth cache even if classified as connection error.
                    clear_auth_cache = self._is_auth_error(e) or attempt >= 1
                    try:
                        self._hard_reconnect(clear_auth_cache=clear_auth_cache)
                    except Exception as reconnect_error:
                        if self.debugger:
                            self.debugger.logger.warning(f"Hard reconnect failed: {reconnect_error}")
                if attempt == max_retries - 1: raise
                time.sleep((2 ** attempt))
        raise Exception("Max retries exceeded")

    def get_domestic_balance(self) -> Dict:
        try:
            is_mock = "openapivts" in self.base_url
            tr_id = "VTTC8434R" if is_mock else "TTTC8434R"
            
            endpoint = "/uapi/domestic-stock/v1/trading/inquire-balance"
            headers = self._get_headers(tr_id)
            params = {"CANO": self.account_number, "ACNT_PRDT_CD": self.account_code, "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""}
            data = self._api_call_with_retry("GET", endpoint, headers, params)
            
            output2 = data.get("output2")
            total_krw = 0
            if isinstance(output2, list) and len(output2) > 0:
                total_krw = int(float(output2[0].get("tot_evlu_amt", 0)))

            stocks = [
                AssetInfo(
                    symbol=i.get("pdno",""), 
                    name=i.get("prdt_name",""), 
                    quantity=int(i.get("hldg_qty",0)), 
                    average_price=float(i.get("pchs_avg_pric",0)), 
                    current_price=float(i.get("prpr",0)), 
                    eval_amount=int(float(i.get("evlu_amt",0))),
                    eval_amount_usd=0, # 국내 주식은 USD 평가액 없음
                    market_type="domestic"
                ) for i in data.get("output1",[]) if int(i.get("hldg_qty",0)) > 0
            ]
            if self.debugger: self.debugger.logger.info(f"Domestic balance: {total_krw:,} KRW, {len(stocks)} stocks (tr_id: {tr_id})")
            return {"total_krw": total_krw, "stocks": stocks}
        except Exception as e:
            if self.debugger: self.debugger.logger.error(f"Domestic balance query failed: {e}")
            return {"total_krw": 0, "stocks": []}

    def get_overseas_balance(self) -> Dict:
        """해외 주식 잔고 조회 (공식 문서 기반 최종 수정)"""
        all_stocks = []
        is_mock = "openapivts" in self.base_url
        tr_id = "VTTS3012R" if is_mock else "TTTS3012R"
        
        for exch_code in ["NYS", "NAS", "AMEX", "ARCX"]:
            try:
                endpoint = "/uapi/overseas-stock/v1/trading/inquire-balance"
                headers = self._get_headers(tr_id)
                params = {
                    "CANO": self.account_number, "ACNT_PRDT_CD": self.account_code,
                    "OVRS_EXCG_CD": exch_code, "TR_CRCY_CD": "USD",
                    "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""
                }
                data = self._api_call_with_retry("GET", endpoint, headers, params)
                
                output1 = data.get("output1", [])
                for item in output1:
                    if int(item.get("ovrs_cblc_qty", 0)) > 0:
                        stock_info = AssetInfo(
                            symbol=item.get("ovrs_pdno", ""),
                            name=item.get("ovrs_item_name", ""),
                            quantity=int(item.get("ovrs_cblc_qty", 0)),
                            average_price=float(item.get("pchs_avg_pric", 0)),
                            current_price=float(item.get("now_pric2", 0)),
                            # 공식 문서 기준, 개별 주식의 USD 평가액은 ovrs_stck_evlu_amt
                            eval_amount_usd=float(item.get("ovrs_stck_evlu_amt", 0)),
                            # 원화 평가액은 이 API에서 제공하지 않으므로 0으로 처리
                            eval_amount=0, 
                            market_type=f"overseas_{exch_code}"
                        )
                        all_stocks.append(stock_info)
                
                if self.debugger: self.debugger.logger.info(f"Successfully fetched {exch_code} balance.")
            except Exception as e:
                if self.debugger: self.debugger.logger.warning(f"Could not fetch {exch_code} balance: {e}")

        # 총 USD 평가액은 모든 개별 주식의 USD 평가액을 합산하여 계산
        total_usd = sum(stock.eval_amount_usd for stock in all_stocks)

        if self.debugger:
            self.debugger.logger.info(f"Overseas balance query completed: {total_usd:,.2f} USD, {len(all_stocks)} stocks (tr_id: {tr_id})")
            
        # 이 API는 신뢰할 수 있는 원화 총액을 제공하지 않으므로, USD 총액만 반환
        return {"total_usd": total_usd, "stocks": all_stocks}

    def fetch_current_price(self, exchange: str, ticker: str) -> Optional[float]:
        """해외 주식 현재가 조회"""
        is_mock = "openapivts" in self.base_url
        tr_id = "HHDFS00000300" if not is_mock else "VHDFS00000300"
        
        exchange_code = self.query_exchange_code.get(exchange)
        if not exchange_code:
            if self.debugger: self.debugger.logger.error(f"Invalid exchange for fetching price: {exchange}")
            return None

        endpoint = "/uapi/overseas-price/v1/quotations/price"
        headers = self._get_headers(tr_id)
        params = {"AUTH": "", "EXCD": exchange_code.value, "SYMB": ticker}
        
        try:
            data = self._api_call_with_retry("GET", endpoint, headers, params)
            price_str = data.get("output", {}).get("last", "0")
            price = float(price_str)
            if self.debugger: self.debugger.logger.info(f"Current price for {ticker} ({exchange}): {price}")
            return price
        except Exception as e:
            if self.debugger: self.debugger.logger.error(f"Failed to fetch current price for {ticker}: {e}")
            return None

    @validate_arguments
    def create_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        order_type: Literal["limit", "market"],
        side: Literal["buy", "sell"],
        amount: int,
        price: int = 0,
        mintick=0.01,
    ):
        endpoint = (
            Endpoints.korea_order.value
            if exchange == "KRX"
            else Endpoints.usa_order.value
        )
        body = self.base_order_body.dict()
        headers = copy.deepcopy(self.base_headers)
        price = str(price)

        amount = str(int(amount))

        if exchange == "KRX":
            headers |= (
                KoreaBuyOrderHeaders(**headers).dict()
                if side == "buy"
                else KoreaSellOrderHeaders(**headers).dict()
            )

            if order_type == "market":
                body |= KoreaMarketOrderBody(**body, PDNO=ticker, ORD_QTY=amount).dict()
            elif order_type == "limit":
                body |= KoreaOrderBody(
                    **body,
                    PDNO=ticker,
                    ORD_DVSN=KoreaOrderType.limit,
                    ORD_QTY=amount,
                    ORD_UNPR=price,
                ).dict()
        elif exchange in ("NASDAQ", "NYSE", "AMEX"):
            exchange_code = self.order_exchange_code.get(exchange)
            
            # 시장가 주문을 위해 현재가 조회
            if order_type == "market":
                current_price = self.fetch_current_price(exchange, ticker)
                if current_price is None:
                    log_message(f"Failed to fetch current price for {ticker}, order cancelled.")
                    return None # 현재가 조회가 안되면 주문 불가
                
                # 시장가 주문 시, 지정가 주문으로 변환 (매수: 5% 상향, 매도: 5% 하향)
                price_multiplier = 1.05 if side == "buy" else 0.95
                order_price = round(current_price * price_multiplier, 2)

                if order_price < 0.01: order_price = 0.01
            else: # 지정가 주문
                order_price = float(price)

            headers |= (
                UsaBuyOrderHeaders(**headers).dict()
                if side == "buy"
                else UsaSellOrderHeaders(**headers).dict()
            )

            # 모든 해외주문은 지정가(UsaOrderType.limit)로 처리
            body |= UsaOrderBody(
                **body,
                PDNO=ticker,
                ORD_DVSN=UsaOrderType.limit.value,
                ORD_QTY=amount,
                OVRS_ORD_UNPR=f"{order_price:.2f}",
                OVRS_EXCG_CD=exchange_code.value,
            ).dict()
        # 주문 API 호출
        return self._api_call_with_retry("POST", endpoint, headers=headers, data=body)

    def create_market_buy_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        amount: int,
        price: int = 0,
    ):
        if exchange == "KRX":
            return self.create_order(exchange, ticker, "market", "buy", amount)
        elif exchange == "usa":
            return self.create_order(exchange, ticker, "market", "buy", amount, price)

    def create_market_sell_order(
        self,
        exchange: Literal["KRX", "NASDAQ", "NYSE", "AMEX"],
        ticker: str,
        amount: int,
        price: int = 0,
    ):
        if exchange == "KRX":
            return self.create_order(exchange, ticker, "market", "sell", amount)
        elif exchange == "usa":
            return self.create_order(exchange, ticker, "market", "buy", amount, price)

    def get_exchange_rate(self) -> Optional[float]:
        """USD/KRW 환율 조회 (다중 소스 백업)"""
        # 1순위: 한국수출입은행 API (oapi.koreaexim.go.kr, JSON 형식)
        try:
            auth_key = "cW2I7v06dvyLw4QU4UQQEG3PBOpU9b8U"
            search_date = datetime.now().strftime('%Y%m%d')
            url = f"https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={auth_key}&searchdate={search_date}&data=AP01"
            
            response = httpx.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            for item in data:
                if item.get('cur_unit') == 'USD':
                    exchange_rate_str = item.get('tts', item.get('deal_bas_r', '0')).replace(',', '')
                    if float(exchange_rate_str) > 0:
                        if self.debugger: self.debugger.logger.info(f"Successfully fetched exchange rate from Eximbank (JSON): {exchange_rate_str}")
                        return float(exchange_rate_str)
            raise ValueError("USD exchange rate not found in Eximbank API response")

        except Exception as e:
            if self.debugger: self.debugger.logger.warning(f"Eximbank API failed: {e}. Trying Naver Finance.")
            # 2순위: 네이버 금융 크롤링
            try:
                url = "https://finance.naver.com/marketindex/"
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = httpx.get(url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                exchange_rate_str = soup.select_one("#exchangeList > li.on > a.head.usd > div > span.value").text.replace(',', '')
                if self.debugger: self.debugger.logger.info(f"Successfully fetched exchange rate from Naver Finance: {exchange_rate_str}")
                return float(exchange_rate_str)
            except Exception as e_naver:
                if self.debugger: self.debugger.logger.error(f"Naver Finance scraping failed: {e_naver}. Using fallback rate.")
                # 3순위: 고정 환율
                return None

    def get_balance(self) -> Dict:
        """국내/해외 자산 통합 조회"""
        try:
            if self.debugger: self.debugger.logger.info(f"Starting balance query for KIS{self.kis_number}")
            domestic = self.get_domestic_balance()
            time.sleep(0.5)
            overseas = self.get_overseas_balance()
            time.sleep(0.5)
            
            is_rate_fallback = False
            exchange_rate = self.get_exchange_rate()
            if exchange_rate is None:
                if self.debugger: self.debugger.logger.warning("Live exchange rate unavailable, using fallback rate 1350.0.")
                exchange_rate = 1350.0
                is_rate_fallback = True

            result = {
                "domestic_balance": domestic,
                "overseas_balance": overseas,
                "exchange_rate": exchange_rate,
                "is_rate_fallback": is_rate_fallback
            }
            
            if self.debugger: 
                dom_krw = domestic.get('total_krw', 0)
                ovs_usd = overseas.get('total_usd', 0)
                self.debugger.logger.info(f"Balance query completed: Domestic {dom_krw:,} KRW, Overseas ${ovs_usd:,.2f}, Rate: {exchange_rate} (Fallback: {is_rate_fallback})")
            return result
        except Exception as e:
            if self.debugger: self.debugger.logger.error(f"Balance query failed: {e}\n{traceback.format_exc()}")
            return {"domestic_balance": {}, "overseas_balance": {}, "exchange_rate": 1350.0, "is_rate_fallback": True}

    def health_check(self) -> Dict:
        try:
            token_info = {}
            if self.debugger and self.token_expires_at:
                token_info = self.debugger.debug_token_validity(self.access_token or "", self.token_expires_at.strftime("%Y-%m-%d %H:%M:%S"))
            api_status = "healthy" if self._validate_token() else "error: token validation failed"
            return {"kis_number": self.kis_number, "timestamp": datetime.now().isoformat(), "token_status": token_info, "api_status": api_status, "recent_api_calls": len([t for t in self.api_call_times if time.time() - t < 60])}
        except Exception as e:
            return {"kis_number": self.kis_number, "status": "error", "error": str(e)}

    def close(self):
        if hasattr(self, 'session'): self.session.close()
        if self.debugger: self.debugger.logger.info(f"KIS{self.kis_number} session closed")
