"""
==============================================================================
MOLTY ROYALE BOT - API CLIENT
==============================================================================
"""

import time
import logging
import requests
from typing import Optional, Dict, Any

logger = logging.getLogger("MoltyBot.API")


class APIError(Exception):
    def __init__(self, message: str, code: str = "UNKNOWN"):
        self.code = code
        super().__init__(f"[{code}] {message}")


class APIClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-API-Key": api_key
        })
        self._request_count = 0
        self._last_request_time = 0.0

    def _request(self, method: str, path: str, max_retries: int = 3,
                 retry_delay: float = 2.0, timeout: int = 10, **kwargs) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_error = None
        for attempt in range(max_retries):
            try:
                elapsed = time.time() - self._last_request_time
                if elapsed < 0.1:
                    time.sleep(0.1 - elapsed)
                response = self.session.request(method, url, timeout=timeout, **kwargs)
                self._last_request_time = time.time()
                self._request_count += 1
                data = response.json()
                if not data.get("success", True):
                    error = data.get("error", {})
                    code  = error.get("code", "UNKNOWN")
                    msg   = error.get("message", "Unknown API error")
                    non_retryable = {
                        "AGENT_NOT_FOUND", "GAME_NOT_FOUND", "GAME_ALREADY_STARTED",
                        "ACCOUNT_ALREADY_IN_GAME", "ONE_AGENT_PER_API_KEY",
                        "INSUFFICIENT_BALANCE", "GEO_RESTRICTED", "ALREADY_ACTED",
                        "INSUFFICIENT_EP", "INVALID_ACTION", "MAX_AGENTS_REACHED"
                    }
                    if code in non_retryable:
                        raise APIError(msg, code)
                    last_error = APIError(msg, code)
                    if attempt < max_retries - 1:
                        logger.warning(f"API error {code}, retry {attempt+1}/{max_retries}")
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                    raise last_error
                return data
            except APIError:
                raise
            except requests.exceptions.Timeout:
                last_error = APIError("Request timeout", "TIMEOUT")
                if attempt >= max_retries - 1:
                    logger.warning(f"Timeout on {path} (final attempt {attempt+1}/{max_retries})")
                else:
                    logger.debug(f"Timeout on {path}, retrying... ({attempt+1}/{max_retries})")
                time.sleep(retry_delay)
            except requests.exceptions.ConnectionError:
                last_error = APIError("Connection error", "CONNECTION_ERROR")
                if attempt >= max_retries - 1:
                    logger.warning(f"Connection error on {path} (final attempt {attempt+1}/{max_retries})")
                else:
                    logger.debug(f"Connection error on {path}, retrying... ({attempt+1}/{max_retries})")
                time.sleep(retry_delay * 2)
            except Exception as e:
                last_error = APIError(str(e), "UNEXPECTED")
                logger.error(f"Unexpected error on {path}: {e}")
                time.sleep(retry_delay)
        raise last_error or APIError("Max retries exceeded", "MAX_RETRIES")

    def get(self, path: str, **kwargs) -> Dict[str, Any]:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, json: Dict = None, **kwargs) -> Dict[str, Any]:
        return self._request("POST", path, json=json, **kwargs)

    def put(self, path: str, json: Dict = None, **kwargs) -> Dict[str, Any]:
        return self._request("PUT", path, json=json, **kwargs)

    def create_account(self, name: str = None) -> Dict:
        payload = {}
        if name:
            payload["name"] = name
        return self.post("/accounts", json=payload)["data"]

    def get_account(self) -> Dict:
        response = self.get("/accounts/me")
        return response.get("data") or response

    def set_wallet(self, wallet_address: str) -> Dict:
        response = self.put("/accounts/wallet", json={"wallet_address": wallet_address})
        return response.get("data") or response

    def get_history(self, limit: int = 50) -> list:
        return self.get(f"/accounts/history?limit={limit}").get("data", [])

    def list_games(self, status: str = "waiting") -> list:
        try:
            return self.get(f"/games?status={status}", timeout=8).get("data", [])
        except (APIError, Exception):
            return []

    def list_games_fast(self, status: str = "waiting") -> list:
        try:
            return self._request(
                "GET", f"/games?status={status}",
                max_retries=1, timeout=3, retry_delay=0
            ).get("data", [])
        except Exception:
            return []

    def get_game(self, game_id: str) -> Dict:
        return self.get(f"/games/{game_id}")["data"]

    def create_game(self, host_name: str = None, map_size: str = "medium",
                    entry_type: str = "free", max_agents: int = None) -> Dict:
        payload = {"mapSize": map_size, "entryType": entry_type}
        if host_name:
            payload["hostName"] = host_name
        if max_agents:
            payload["maxAgents"] = max_agents
        return self.post("/games", json=payload)["data"]

    def register_agent(self, game_id: str, agent_name: str) -> Dict:
        return self.post(
            f"/games/{game_id}/agents/register",
            json={"name": agent_name}
        )["data"]

    def register_agent_fast(self, game_id: str, agent_name: str) -> Dict:
        return self._request(
            "POST", f"/games/{game_id}/agents/register",
            max_retries=1, timeout=5, retry_delay=0,
            json={"name": agent_name}
        )["data"]

    def get_state(self, game_id: str, agent_id: str) -> Dict:
        return self.get(f"/games/{game_id}/agents/{agent_id}/state")["data"]

    def take_action(self, game_id: str, agent_id: str,
                    action: Dict, thought: Dict = None) -> Dict:
        payload = {"action": action}
        if thought:
            payload["thought"] = thought
        try:
            result = self.post(
                f"/games/{game_id}/agents/{agent_id}/action",
                json=payload
            )
            return result
        except APIError as e:
            if e.code == "ALREADY_ACTED":
                logger.debug("Already acted this turn, skipping")
                return {"success": False, "error": {"code": e.code}}
            raise

    def move(self, game_id: str, agent_id: str, region_id: str, thought: str = None) -> Dict:
        action = {"type": "move", "regionId": region_id}
        t = {"reasoning": thought or "Moving to region", "plannedAction": "move"}
        return self.take_action(game_id, agent_id, action, t)

    def explore(self, game_id: str, agent_id: str, thought: str = None) -> Dict:
        action = {"type": "explore"}
        t = {"reasoning": thought or "Exploring region", "plannedAction": "explore"}
        return self.take_action(game_id, agent_id, action, t)

    def attack(self, game_id: str, agent_id: str, target_id: str,
               target_type: str = "agent", thought: str = None) -> Dict:
        action = {"type": "attack", "targetId": target_id, "targetType": target_type}
        t = {"reasoning": thought or f"Attacking {target_type}", "plannedAction": "attack"}
        return self.take_action(game_id, agent_id, action, t)

    def use_item(self, game_id: str, agent_id: str, item_id: str, thought: str = None) -> Dict:
        action = {"type": "use_item", "itemId": item_id}
        t = {"reasoning": thought or "Using item", "plannedAction": "use_item"}
        return self.take_action(game_id, agent_id, action, t)

    def interact(self, game_id: str, agent_id: str, interactable_id: str, thought: str = None) -> Dict:
        action = {"type": "interact", "interactableId": interactable_id}
        t = {"reasoning": thought or "Using facility", "plannedAction": "interact"}
        return self.take_action(game_id, agent_id, action, t)

    def rest(self, game_id: str, agent_id: str) -> Dict:
        action = {"type": "rest"}
        t = {"reasoning": "Resting to recover EP", "plannedAction": "rest"}
        return self.take_action(game_id, agent_id, action, t)

    def pickup(self, game_id: str, agent_id: str, item_id: str) -> Dict:
        return self.take_action(game_id, agent_id, {"type": "pickup", "itemId": item_id})

    def equip(self, game_id: str, agent_id: str, item_id: str) -> Dict:
        return self.take_action(game_id, agent_id, {"type": "equip", "itemId": item_id})

    def talk(self, game_id: str, agent_id: str, message: str) -> Dict:
        return self.take_action(game_id, agent_id, {"type": "talk", "message": message[:200]})

    def whisper(self, game_id: str, agent_id: str, target_id: str, message: str) -> Dict:
        return self.take_action(game_id, agent_id, {
            "type": "whisper", "targetId": target_id, "message": message[:200]
        })
