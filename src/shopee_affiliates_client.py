import os
import time
import json
import hashlib
import requests
from dotenv import load_dotenv


class ShopeeAffiliatesClientError(Exception):
    pass


class ShopeeAffiliatesClient:
    """
    Shopee Affiliates Open API (GraphQL)

    Endpoint:
      https://open-api.affiliate.shopee.com.br/graphql

    Authorization header:
      Authorization: SHA256 Credential=<APP_ID>, Signature=<SIGN>, Timestamp=<TS>

    Signature (correto):
      SHA256(AppId + Timestamp + Payload + Secret)
    """

    def __init__(self, base_url: str, app_id: str, secret: str, timeout_s: int = 20):
        self.base_url = (base_url or "").strip()
        self.app_id = (app_id or "").strip()
        self.secret = (secret or "").strip()
        self.timeout_s = timeout_s

        if not self.base_url or not self.app_id or not self.secret:
            raise ShopeeAffiliatesClientError(
                "Config inválida: base_url/app_id/secret não podem estar vazios."
            )

        self.session = requests.Session()

    @staticmethod
    def from_env() -> "ShopeeAffiliatesClient":
        load_dotenv()
        base_url = os.getenv("SHOPEE_AFF_BASE_URL", "").strip()
        app_id = os.getenv("SHOPEE_AFF_APP_ID", "").strip()
        secret = os.getenv("SHOPEE_AFF_SECRET", "").strip()
        timeout_s = int(os.getenv("SHOPEE_AFF_TIMEOUT_S", "20").strip() or "20")

        if not base_url or not app_id or not secret:
            raise ShopeeAffiliatesClientError(
                "Faltam variáveis no .env. Precisa de:\n"
                "- SHOPEE_AFF_BASE_URL\n"
                "- SHOPEE_AFF_APP_ID\n"
                "- SHOPEE_AFF_SECRET\n"
            )

        return ShopeeAffiliatesClient(base_url, app_id, secret, timeout_s=timeout_s)

    def _make_payload(self, query: str, variables: dict | None) -> str:
        # IMPORTANTE: o payload assinado deve ser exatamente o que você envia no POST.
        payload_dict = {
            "query": query,
            "variables": variables or {}
        }
        # JSON compacto (sem espaços) ajuda a bater com o esperado
        return json.dumps(payload_dict, separators=(",", ":"), ensure_ascii=False)

    def _sign(self, payload: str, timestamp: int) -> str:
        # Signature = SHA256(AppId + Timestamp + Payload + Secret)
        base_string = f"{self.app_id}{timestamp}{payload}{self.secret}"
        return hashlib.sha256(base_string.encode("utf-8")).hexdigest()

    def _headers(self, payload: str) -> dict:
        timestamp = int(time.time())  # segundos (não ms)
        signature = self._sign(payload, timestamp)

        return {
            "Content-Type": "application/json",
            "Authorization": (
                f"SHA256 Credential={self.app_id}, "
                f"Signature={signature}, "
                f"Timestamp={timestamp}"
            ),
        }

    def execute(self, query: str, variables: dict | None = None) -> dict:
        payload = self._make_payload(query, variables)
        headers = self._headers(payload)

        debug = os.getenv("SHOPEE_AFF_DEBUG", "0").strip() == "1"
        if debug:
            print("DEBUG BASE_URL:", self.base_url)
            print("DEBUG APP_ID_LEN:", len(self.app_id))
            print("DEBUG SECRET_LEN:", len(self.secret))
            print("DEBUG PAYLOAD:", payload[:120] + ("..." if len(payload) > 120 else ""))

        resp = self.session.post(
            self.base_url,
            headers=headers,
            data=payload,
            timeout=self.timeout_s,
        )

        if resp.status_code != 200:
            raise ShopeeAffiliatesClientError(f"HTTP {resp.status_code}: {resp.text}")

        data = resp.json()

        if "errors" in data and data["errors"]:
            raise ShopeeAffiliatesClientError(f"GraphQL Error: {data['errors']}")

        return data.get("data", {})
