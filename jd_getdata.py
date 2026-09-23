# -*- coding: utf-8 -*-
"""Download one day of JD traffic-source data."""

import hashlib
import json
import re
import time
import uuid as uuid_module
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

try:
    from xbot import print as xbot_print
except ImportError:
    xbot_print = print


class JdFlowSourceDownloader:
    """Submit, poll, and download one JD flow-source report."""

    SUBMIT_URL = "https://szgateway.jd.com/api/lowcode/bff/download/flowSource/table/offlineProductTable.ajax"
    LANDING_URL = "https://jdsz.jd.com/szweb/view/index/home.html"
    IDENTITIES_URL = "https://szgateway.jd.com/api/common/getUserIdentities.ajax"
    TASK_LIST_URL = "https://szgateway.jd.com/api/common/downloadCenter/getTaskList.ajax"
    FILE_LINK_URL = "https://szgateway.jd.com/api/common/downloadCenter/getFileLink.ajax"
    REFERER = "https://jdsz.jd.com/szweb/view/view-source/view-flow.html"
    DOWNLOAD_REFERER = "https://jdsz.jd.com/szweb/view/reports-center/download-center.html"
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
    )
    OUTPUT_DIR = Path(r"D:\品牌数据")

    INDICATORS = [
        "jdr_sch_traffic_brow_sku_cnt_jd_unified_attribution_sz",
        "jdr_sch_traffic_brow_sku_cnt_jd_unified_attribution_sz##compare",
        "jdr_sch_traffic_brow_sku_cnt_jd_unified_attribution_sz##compareValue",
        "jdr_sch_traffic_brow_sku_cnt_jd_unified_attribution_sz/jdr_sch_traffic_brow_sku__page_cnt_traffic_plat_item_di_sz_bsg##customProportion",
        "jdr_sch_traffic_brow_sku_cnt_jd_unified_attribution_sz/jdr_sch_traffic_brow_sku__page_cnt_traffic_plat_item_di_sz_bsg##customProportionCompare",
        "jdr_sch_traffic_brow_sku_cnt_jd_unified_attribution_sz/jdr_sch_traffic_brow_sku__page_cnt_traffic_plat_item_di_sz_bsg##customProportionCompareValue",
        "jdr_sch_traffic_brow_sku_qtty_jd_unified_attribution_sz",
        "jdr_sch_traffic_brow_sku_qtty_jd_unified_attribution_sz##compare",
        "jdr_sch_traffic_brow_sku_qtty_jd_unified_attribution_sz##compareValue",
        "fo_jdr_sch_traffic_per_pv_jd_unified_attribution_sz",
        "fo_jdr_sch_traffic_per_pv_jd_unified_attribution_sz##compare",
        "fo_jdr_sch_traffic_per_pv_jd_unified_attribution_sz##compareValue",
        "fo_jdr_sch__item_detail_view_avg_stay_duration_per_jd_unified_attribution_sz",
        "fo_jdr_sch__item_detail_view_avg_stay_duration_per_jd_unified_attribution_sz##compare",
        "fo_jdr_sch__item_detail_view_avg_stay_duration_per_jd_unified_attribution_sz##compareValue",
        "fo_jdr_sch_traffic_uv_value_jd_unified_attribution_sz",
        "fo_jdr_sch_traffic_uv_value_jd_unified_attribution_sz##compare",
        "fo_jdr_sch_traffic_uv_value_jd_unified_attribution_sz##compareValue",
        "fo_jdr_sch_traffic_arpu_jd_unified_attribution_sz",
        "fo_jdr_sch_traffic_arpu_jd_unified_attribution_sz##compare",
        "fo_jdr_sch_traffic_arpu_jd_unified_attribution_sz##compareValue",
        "jdr_sch_traffic_intr_ord_ord_cnt_jd_unified_attribution_trade_deal_snapshot_sz",
        "jdr_sch_traffic_intr_ord_ord_cnt_jd_unified_attribution_trade_deal_snapshot_sz##compare",
        "jdr_sch_traffic_intr_ord_ord_cnt_jd_unified_attribution_trade_deal_snapshot_sz##compareValue",
        "jdr_sch_traffic_intr_ord_ord_amt_jd_unified_attribution_trade_deal_snapshot_sz",
        "jdr_sch_traffic_intr_ord_ord_amt_jd_unified_attribution_trade_deal_snapshot_sz##compare",
        "jdr_sch_traffic_intr_ord_ord_amt_jd_unified_attribution_trade_deal_snapshot_sz##compareValue",
        "jdr_sch_traffic_intr_ord_sku_qtty_jd_unified_attribution_trade_deal_snapshot_sz",
        "jdr_sch_traffic_intr_ord_sku_qtty_jd_unified_attribution_trade_deal_snapshot_sz##compare",
        "jdr_sch_traffic_intr_ord_sku_qtty_jd_unified_attribution_trade_deal_snapshot_sz##compareValue",
        "jdr_sch_traffic_intr_ord_ord_qtty_jd_unified_attribution_trade_deal_snapshot_sz",
        "jdr_sch_traffic_intr_ord_ord_qtty_jd_unified_attribution_trade_deal_snapshot_sz##compare",
        "jdr_sch_traffic_intr_ord_ord_qtty_jd_unified_attribution_trade_deal_snapshot_sz##compareValue",
        "fo_jdr_sch_fo_jdr_sch_traffic_intr_ord_cvr_deal_sz",
        "fo_jdr_sch_fo_jdr_sch_traffic_intr_ord_cvr_deal_sz##compare",
        "fo_jdr_sch_fo_jdr_sch_traffic_intr_ord_cvr_deal_sz##compareValue",
        "fo_jdr_sch_traffic_intr_ord_cvr_deal_pv_sz",
        "fo_jdr_sch_traffic_intr_ord_cvr_deal_pv_sz##compare",
        "fo_jdr_sch_traffic_intr_ord_cvr_deal_pv_sz##compareValue",
    ]
    SOURCE_FIELDS = [
        "jdr_sch_traffic_cha_last_field_src_rmad_sz_1",
        "jdr_sch_traffic_cha_last_field_src_rmad_sz_2",
        "jdr_sch_traffic_cha_last_field_src_rmad_sz_3",
        "jdr_sch_traffic_cha_last_field_src_rmad_sz_4",
    ]

    def __init__(self, cookie, output_dir=None, timeout=30, poll_interval=3,
                 poll_timeout=300):
        """Create a client using the explicitly supplied JD Cookie."""
        self.cookie = str(cookie or "").strip()
        if not self.cookie:
            raise ValueError("cookie 不能为空")
        self.output_dir = Path(output_dir or self.OUTPUT_DIR)
        self.timeout = int(timeout)
        self.poll_interval = max(1, int(poll_interval))
        self.poll_timeout = max(self.poll_interval, int(poll_timeout))
        # JD may reject the TLS fingerprint of urllib3/requests.  When the
        # optional dependency is installed, impersonate Chrome automatically.
        self.session = (
            curl_requests.Session(impersonate="chrome")
            if curl_requests is not None
            else requests.Session()
        )
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Origin": "https://jdsz.jd.com",
            "Priority": "u=1, i",
            "Sec-CH-UA": '"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": self.USER_AGENT,
            "X-Requested-With": "XMLHttpRequest",
        })
        # Load the supplied browser Cookie into a jar so Set-Cookie responses from
        # JD's bootstrap endpoints are retained for the subsequent POST.
        for part in self.cookie.split(";"):
            name, separator, value = part.strip().partition("=")
            if separator and name.strip():
                self.session.cookies.set(name.strip(), value.strip())

    @staticmethod
    def _validate_date(value):
        """Validate and normalize the input date as YYYY-MM-DD."""
        if isinstance(value, (date, datetime)):
            return value.strftime("%Y-%m-%d")
        text = str(value or "").strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        raise ValueError("endDate 必须是 YYYY-MM-DD、YYYY/MM/DD 或 YYYYMMDD")

    def _headers(self, url, referer):
        """Build the three request metadata headers used by JD's web client."""
        request_uuid = str(uuid_module.uuid4())
        user_mup = str(int(time.time() * 1000))
        pathname = urlparse(url).path
        sign_text = f"{pathname}{request_uuid}{user_mup}372ad2c2b6"
        user_mnp = hashlib.md5(sign_text.encode("utf-8")).hexdigest()
        headers = {
            "Referer": referer,
            "User-mnp": user_mnp,
            "User-mup": user_mup,
            "uuid": request_uuid,
        }
        return headers

    @staticmethod
    def _check_payload(payload, endpoint):
        """Raise only when the JD response explicitly reports failure."""
        if not isinstance(payload, dict):
            return
        success = payload.get("success")
        code = payload.get("code")
        header = payload.get("header") if isinstance(payload.get("header"), dict) else {}
        if code in (None, ""):
            code = header.get("code")
        if success is False:
            raise RuntimeError(f"京东接口失败（{endpoint}）：{payload.get('msg') or payload.get('message') or payload}")
        if code not in (None, "", 0, "0", 200, "200", "success", "SUCCESS"):
            description = header.get("desc") or payload.get("msg") or payload.get("message")
            if str(code) == "-407" or "\u4e0d\u5b89\u5168" in str(description or ""):
                raise RuntimeError(
                    f"JD rejected the request ({endpoint}, code={code}); "
                    "the dynamic user-mnp signature was sent correctly, so check whether the Cookie is current "
                    "and whether JD has triggered an account/IP risk challenge."
                )
            raise RuntimeError(f"京东接口返回错误（{endpoint}）：{description or payload}")

    def _json_request(self, method, url, endpoint, **kwargs):
        """Send an HTTP request and decode its JSON response."""
        response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        try:
            payload = response.json()
        except ValueError as exc:
            response.raise_for_status()
            raise RuntimeError(f"{endpoint} 未返回 JSON：{response.text[:300]}") from exc
        self._check_payload(payload, endpoint)
        response.raise_for_status()
        return payload

    @staticmethod
    def _walk_dicts(value):
        """Yield every dictionary nested in a JD response."""
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from JdFlowSourceDownloader._walk_dicts(child)
        elif isinstance(value, list):
            for child in value:
                yield from JdFlowSourceDownloader._walk_dicts(child)
        elif isinstance(value, str):
            text = value.strip()
            if text.startswith(("{", "[")):
                try:
                    decoded = json.loads(text)
                except (TypeError, ValueError):
                    return
                if decoded != value:
                    yield from JdFlowSourceDownloader._walk_dicts(decoded)

    @staticmethod
    def _first_value(mapping, keys):
        """Return the first non-empty value for a list of possible API keys."""
        if not isinstance(mapping, dict):
            return None
        for key in keys:
            value = mapping.get(key)
            if value not in (None, ""):
                return value
        wanted = {str(key).lower().replace("_", "") for key in keys}
        for key, value in mapping.items():
            if str(key).lower().replace("_", "") in wanted and value not in (None, ""):
                return value
        return None

    @classmethod
    def _unwrap_task_name(cls, value):
        """Return the task name inside an optional pair of full-width brackets."""
        if not isinstance(value, str):
            return ""
        match = re.search(r"【([^【】]+)】", value)
        return (match.group(1) if match else value).strip()

    @classmethod
    def _task_name_from_response(cls, payload):
        """Extract the actual task name between 【 and 】 from submit response."""
        for record in cls._walk_dicts(payload):
            value = record.get("taskName") if isinstance(record, dict) else None
            if isinstance(value, str) and re.search(r"【([^【】]+)】", value):
                return cls._unwrap_task_name(value)
        raise RuntimeError("申请下载响应中未找到 taskName 的【】内容")

    @classmethod
    def _unwrap_task_name(cls, value):
        """Unwrap a task name without relying on source-file character encoding."""
        if not isinstance(value, str):
            return ""
        left = value.find("\u3010")
        if left >= 0:
            right = value.find("\u3011", left + 1)
            if right > left:
                return value[left + 1:right].strip()
        return value.strip()

    @classmethod
    def _task_name_from_response(cls, payload):
        """Read taskName from the submit response, including nested JSON strings."""
        for record in cls._walk_dicts(payload):
            value = cls._first_value(record, ("taskName", "task_name", "taskname"))
            if isinstance(value, str) and value.strip():
                return cls._unwrap_task_name(value)
        return ""

    @classmethod
    def _task_id_from_response(cls, payload):
        """Read an optional task ID returned by the submit endpoint."""
        for record in cls._walk_dicts(payload):
            value = cls._first_value(record, ("taskId", "taskID", "task_id"))
            if value not in (None, ""):
                return str(value)
        return ""

    def submit_download(self, end_date):
        """Submit the offline flow-source export task."""
        current = self._validate_date(end_date)
        previous = (datetime.strptime(current, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")
        body = {
            "realtime": False, "interval": "DAY", "dateType": "day",
            "startDate": current, "endDate": current,
            "compareStartDate": previous, "compareEndDate": previous,
            "compareType": "hb", "channel": "app", "spuIds": [],
            "indicators": self.INDICATORS, "sortField": self.INDICATORS[0],
            "sortType": "desc", "pageIndex": 1, "pageSize": 2000,
            "groups": self.SOURCE_FIELDS, "attributes": self.SOURCE_FIELDS,
            "trendType": "segment", "cmpFlag": "0",
        }
        payload = self._json_request(
            "POST", self.SUBMIT_URL, "申请下载",
            headers=self._headers(self.SUBMIT_URL, self.REFERER), json=body
        )
        task_name = self._task_name_from_response(payload)
        task_id = self._task_id_from_response(payload)
        if not task_name and not task_id:
            response_keys = []
            for record in self._walk_dicts(payload):
                if isinstance(record, dict):
                    response_keys.extend(str(key) for key in record.keys())
            key_preview = ", ".join(dict.fromkeys(response_keys))[:500]
            raise RuntimeError(
                f"申请下载接口未返回 taskName 或 taskId，任务可能没有创建成功；响应字段：{key_preview}"
            )
        return {"date": current, "task_name": task_name, "task_id": task_id, "response": payload}

    def bootstrap_session(self):
        """Initialize the logged-in JD page session before creating a task."""
        landing = self.session.get(
            self.LANDING_URL,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://jdsz.jd.com/",
                "User-Agent": self.USER_AGENT,
            },
            timeout=self.timeout,
        )
        landing.raise_for_status()
        return self._json_request(
            "GET",
            self.IDENTITIES_URL,
            "初始化京东会话",
            headers=self._headers(self.IDENTITIES_URL, self.REFERER),
        )

    @staticmethod
    def _status(record):
        """Read a task status, accepting JD's observed spelling variants."""
        value = JdFlowSourceDownloader._first_value(
            record, ("taskStatus", "taskStatues", "askStatues", "status", "statusCode")
        )
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _task_time(record):
        """Extract a sortable creation/update timestamp from a task record."""
        value = JdFlowSourceDownloader._first_value(
            record, ("createTime", "taskCreateTime", "submitTime", "updateTime", "gmtCreate")
        )
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _matching_tasks(self, payload, task_name):
        """Find task records with the exact requested name."""
        records = []
        for record in self._walk_dicts(payload):
            name = self._first_value(record, ("taskName", "task_name", "name"))
            task_id = self._first_value(record, ("taskId", "taskID", "id"))
            if (not task_name or self._unwrap_task_name(name) == task_name) and task_id not in (None, ""):
                records.append(record)
        return records

    def _task_records(self, payload):
        """Extract task records in the exact order returned by JD."""
        records = []
        seen_ids = set()
        for record in self._walk_dicts(payload):
            task_id = self._first_value(record, ("taskId", "taskID", "task_id", "id"))
            task_name = self._first_value(record, ("taskName", "task_name", "name"))
            if task_id in (None, "") or task_name in (None, ""):
                continue
            key = str(task_id)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            records.append(record)
        return records

    def wait_for_first_task(self):
        """Poll until the first task returned by JD reaches status 2."""
        deadline = time.monotonic() + self.poll_timeout
        last_status = None
        last_name = ""
        last_id = ""
        while time.monotonic() < deadline:
            payload = self._json_request(
                "GET", self.TASK_LIST_URL, "查询下载任务",
                headers=self._headers(self.TASK_LIST_URL, self.DOWNLOAD_REFERER),
            )
            records = self._task_records(payload)
            if records:
                record = records[0]
                raw_name = self._first_value(record, ("taskName", "task_name", "name"))
                raw_id = self._first_value(record, ("taskId", "taskID", "task_id", "id"))
                last_name = self._unwrap_task_name(raw_name)
                last_id = str(raw_id)
                last_status = self._status(record)
                if last_status == 2:
                    return {
                        "task_name": last_name,
                        "task_id": last_id,
                        "task_status": last_status,
                    }
            time.sleep(self.poll_interval)
        raise TimeoutError(
            f"任务在 {self.poll_timeout} 秒内未完成：{last_name or '未找到任务'}，"
            f"taskId={last_id or '未知'}，最后状态={last_status}"
        )

    def wait_for_task(self, task_name="", task_id=""):
        """Poll the download center until the submitted task reaches status 2."""
        if not task_name and not task_id:
            raise ValueError("查询任务时必须提供 taskName 或 taskId")
        deadline = time.monotonic() + self.poll_timeout
        last_status = None
        while time.monotonic() < deadline:
            payload = self._json_request(
                "GET", self.TASK_LIST_URL, "查询下载任务",
                headers=self._headers(self.TASK_LIST_URL, self.DOWNLOAD_REFERER),
            )
            records = self._matching_tasks(payload, task_name)
            records.sort(key=self._task_time, reverse=True)
            for record in records:
                record_id = self._first_value(record, ("taskId", "taskID", "task_id", "id"))
                if task_id and str(record_id) != str(task_id):
                    continue
                last_status = self._status(record)
                if last_status == 2:
                    return str(record_id)
            time.sleep(self.poll_interval)
        raise TimeoutError(f"任务在 {self.poll_timeout} 秒内未完成：{task_name}，最后状态={last_status}")

    def _get_file_url_once(self, task_id):
        """Resolve a completed task ID to its downloadable file URL."""
        payload = self._json_request(
            "GET", self.FILE_LINK_URL, "获取下载链接",
            headers=self._headers(self.FILE_LINK_URL, self.DOWNLOAD_REFERER),
            params={"taskId": str(task_id)},
        )
        for record in self._walk_dicts(payload):
            value = self._first_value(record, ("fileUrl", "fileURL", "downloadUrl", "downloadURL"))
            if isinstance(value, str):
                value = value.strip()
                if value.startswith("//"):
                    return "https:" + value
                if value.startswith(("http://", "https://")):
                    return value
        raise RuntimeError(f"任务 {task_id} 的响应中没有找到 fileUrl")

    def get_file_url(self, task_id):
        """Poll until JD publishes the completed task's downloadable URL."""
        deadline = time.monotonic() + self.poll_timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                return self._get_file_url_once(task_id)
            except RuntimeError as exc:
                last_error = exc
                time.sleep(self.poll_interval)
        raise RuntimeError(
            f"任务 {task_id} 在 {self.poll_timeout} 秒内没有返回 fileUrl：{last_error}"
        )

    @staticmethod
    def _extension(response, file_url):
        """Choose a useful extension while preserving the server's format."""
        suffix = Path(urlparse(file_url).path).suffix.lower()
        if suffix in {".xlsx", ".xls", ".csv", ".zip", ".gz"}:
            return suffix
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
        return {
            "application/zip": ".zip",
            "application/vnd.ms-excel": ".xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "text/csv": ".csv",
        }.get(content_type, ".xlsx")

    def download_file(self, file_url, end_date, store_name):
        """Download the completed report into D:\\品牌数据."""
        response = self.session.get(
            file_url,
            headers=self._headers(file_url, self.DOWNLOAD_REFERER),
            timeout=self.timeout,
            stream=True,
        )
        response.raise_for_status()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        safe_store_name = re.sub(r'[<>:"/\\|?*]', "_", str(store_name).strip()).rstrip(" .")
        if not safe_store_name:
            raise ValueError("store_name 不能为空")
        path = self.output_dir / (
            f"{end_date}-{safe_store_name}-京东流量数据{self._extension(response, file_url)}"
        )
        with path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)
        return str(path)

    def run(self, end_date, store_name):
        """Submit one export task, wait for completion, and download its file."""
        current = self._validate_date(end_date)
        store_name = str(store_name or "").strip()
        if not store_name:
            raise ValueError("请传入 store_name")
        self.bootstrap_session()
        submitted = self.submit_download(current)
        task = {
            "task_name": submitted.get("task_name", ""),
            "task_id": submitted.get("task_id", ""),
            "task_status": None,
        }
        task["task_id"] = self.wait_for_task(task["task_name"], task["task_id"])
        task["task_status"] = 2
        file_url = self.get_file_url(task["task_id"])
        file_path = self.download_file(file_url, current, store_name)
        return {
            "date": current,
            "store_name": store_name,
            "task_name": task["task_name"],
            "task_id": task["task_id"],
            "task_status": task["task_status"],
            "file_url": file_url,
            "file_path": file_path,
        }


def main(args=None):
    """Accept {'cookie': ..., 'endDate': ...} and download one day's report."""
    options = args if isinstance(args, dict) else {}
    end_date = options.get("endDate") or options.get("end_date") or options.get("date")
    if not end_date:
        raise ValueError("请传入 endDate，例如 2026-09-03")
    store_name = options.get("store_name") or options.get("storeName")
    if not store_name:
        raise ValueError("请传入 store_name")
    result = JdFlowSourceDownloader(
        cookie=options.get("cookie", ""),
        output_dir=options.get("output_dir") or options.get("outputDir"),
        timeout=options.get("timeout", 30),
        poll_interval=options.get("poll_interval", 3),
        poll_timeout=options.get("poll_timeout", 300),
    ).run(end_date, store_name)
    try:
        xbot_print(result)
    except Exception:
        pass
    return result
