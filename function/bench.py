import asyncio
import collections
import json
import logging
import os
import socket
import sys
import time
import traceback
import typing
import urllib.error
import urllib.request
from asyncio import Task
from asyncio.selector_events import BaseSelectorEventLoop
from timeit import default_timer

import aiohttp.client_exceptions
import async_timeout
import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables.aio import TableClient, TableServiceClient

HTTP_TIMEOUT = 60
JUICE_GET = "/Juice/Users/LogOn?ReturnUrl=%2fjuice"
ALTO_GET = "/Alto/Account/Login?ReturnUrl=%2FAlto"
JUICE_POST = "/Juice/Users/JsonValidateUser"
JUICE_LOGOUT = "/Juice/Users/LogOut"
AYUDA_BMS_API_V2 = "/Ayuda.BMS.API/v2/ServerInfo/Version"

WEB_BENCH_RESULTS = "WebBench"
WEB_BENCH_RESULTS_AVG = "WebBenchAvg"
WEB_BENCH_RESULTS_LATEST = "WebBenchLatest"

SEMAPHORE = 200
AIOHTTP_LIMITS = 100

API_VERSION = "2016-05-31"
ACCEPT_JSON_FORMAT = "application/json"
DATETIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
DATADOG_METRIC_NAME = "ap.instances.webbench"


log_format = "{levelname}: {asctime} {module}.{funcName} @{lineno}:\t{message}"
logger = logging.getLogger(__name__)
if os.environ.get("DEBUGLEVEL", "INFO") == "DEBUG":
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

logger_azure = logging.getLogger("azure")
logger_azure.setLevel(logging.ERROR)

formatter = logging.Formatter(log_format, style="{")
console = logging.StreamHandler(sys.stdout)
console.setFormatter(formatter)
logger.addHandler(console)


def main(mytimer: func.TimerRequest):
    logger.info("Webbench INIT")

    # START
    logger.info("START. Getting instances from Mamba")

    auth_headers = f"Token {os.environ['MAMBA_TOKEN']}"
    instances_api_endpoint = (
        f"{os.environ['INSTANCES_API_ENDPOINT']}?region={os.environ['REGION']}"
    )

    # Getting list of instances from Mamba
    start_all = default_timer()
    raw_instances = get_instances_from_mamba(instances_api_endpoint, auth_headers)
    # Parsing response from Mamba
    instances = [generate_instance_urls(i) for i in raw_instances]
    logger.info(
        f"Got instances in {round(default_timer() - start_all, 2)} sec. "
        f"Starting bench tasks"
    )

    # Benching instances
    start_step = default_timer()
    results = run_bench(instances=instances)

    # Formatting results
    (avgs, instance_results, post_to_mamba, last_results) = format_results_for_inserts(
        results
    )
    logger.info(
        f"Bench complete in {round(default_timer() - start_step, 2)} sec. "
        f"Starting inserts"
    )

    # Inserting results in Azure table.
    start_step = default_timer()
    asyncio.run(
        run_inserts(
            avgs=avgs, instance_results=instance_results, last_results=last_results
        )
    )
    logger.info(f"Inserts complete in {round(default_timer() - start_step, 2)} sec.")

    # Pushing results to Mamba
    start_step = default_timer()
    post_instances_to_mamba(
        endpoint=os.environ["INSTANCES_API_POST_ENDPOINT"],
        auth_headers=auth_headers,
        payload=post_to_mamba,
    )
    logger.info(
        f"Posting to Mamba complete in "
        f"{round(default_timer() - start_step, 2)} sec. "
    )

    # End
    end_all = default_timer() - start_all
    logger.info(f"END in {round(end_all, 2)} sec.")


class Timer:
    """Context manager to time operations"""

    def __init__(self):
        self.start = None
        self.elapsed = None

    def __enter__(self):
        self.start = default_timer()
        return

    def __exit__(self, *args):
        self.elapsed = default_timer() - self.start


def get_instances_from_mamba(endpoint: str, auth_headers: str) -> dict:
    """Gets a list of instances from Mamba"""
    r = urllib.request.Request(endpoint)
    r.add_header("Authorization", auth_headers)
    response = urllib.request.urlopen(r).read()
    return json.loads(response.decode("utf-8"))


def post_instances_to_mamba(endpoint: str, auth_headers: str, payload: dict):
    """Posts results to Mamba"""
    post_data = json.dumps(payload).encode()
    request_post = urllib.request.Request(url=endpoint, method="POST", data=post_data)
    request_post.add_header("Authorization", auth_headers)
    request_post.add_header("content-type", "application/json")
    urllib.request.urlopen(request_post).read()


async def request(
    url: str,
    method: str,
    session: aiohttp.ClientSession,
    timer: Timer,
    semaphore: asyncio.Semaphore,
    data: typing.Optional[dict] = None,
    headers: typing.Optional[dict] = None,
) -> dict:
    """Executes HTTP requests"""
    try:
        with timer:
            with async_timeout.timeout(HTTP_TIMEOUT):
                async with semaphore, session.request(
                    method=method,
                    url=url,
                    data=data,
                    headers=headers,
                ) as response:
                    logger.debug(f"{url} {response.reason} {response.status}")
                    text = await response.text()
                    return {
                        "reason": response.reason,
                        "status": response.status,
                        "text": text,
                        "error": "",
                        "cookies": response.cookies,
                    }

    except asyncio.TimeoutError:
        logger.warning(f"{url}: Timeout")
        return {
            "reason": None,
            "status": None,
            "text": None,
            "error": "Timeout error.",
            "cookies": {},
        }

    except (
        asyncio.TimeoutError,
        aiohttp.client_exceptions.ClientError,
        aiohttp.client_exceptions.ClientResponseError,
        socket.gaierror,
    ):
        error = traceback.format_exc()
        logger.error(f"{url}: {error}")

        return {
            "reason": None,
            "status": None,
            "text": None,
            "error": error,
            "cookies": {},
        }


async def process_instance(
    instance: dict, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
) -> dict:
    """Executes requests to time for a particular instance."""
    timer = Timer()
    # Alto
    response = await request(
        url=instance["alto_url"],
        method="GET",
        session=session,
        timer=timer,
        semaphore=semaphore,
    )
    instance["alto"] = parse_response(response=response, elapsed=timer.elapsed)

    # ayuda_bms_api_v2
    response = await request(
        url=instance["ayuda_bms_api_v2_url"],
        method="GET",
        session=session,
        timer=timer,
        semaphore=semaphore,
        headers={"Authorization": instance["ayuda_bms_api_v2_auth_headers"]},
    )
    instance["ayuda_bms_api_v2"] = parse_response(
        response=response, elapsed=timer.elapsed
    )

    # Juice get
    response = await request(
        url=instance["juice_url"],
        method="GET",
        session=session,
        timer=timer,
        semaphore=semaphore,
    )
    instance["juice"] = parse_response(response=response, elapsed=timer.elapsed)

    # Juice login
    # No response/cookie, skipping login
    if not response:
        instance["juice_login"] = parse_response(
            response=None,
            elapsed=None,
            exception="Failed to obtain cookie. " "Skipped",
        )
        logger.warning("Failed to obtain cookie " + instance["fqdn"])
        return instance

    # Grabbing cookie values for login test
    login_dict = {
        "username": instance["username"],
        "password": instance["password"],
        "rememberMe": True,
        "returnUrl": "/",
    }
    for k, v in response["cookies"].items():
        if "RequestVerificationToken" in k:
            csrf_raw_value = response["cookies"].get(k).OutputString()
            csrf_value = csrf_raw_value.replace(k + "=", "").split(";", 1)[0]
            login_dict[k] = csrf_value
            break

    response = await request(
        url=instance["juice_login_url"],
        method="POST",
        session=session,
        timer=timer,
        data=login_dict,
        semaphore=semaphore,
    )
    if not response:
        error = "Failed to connect to host."
        instance["juice_login"] = parse_response(
            response=None, elapsed=None, exception=error
        )
        logger.warning(error + " " + instance["fqdn"])
        return instance

    try:
        success = json.loads(response["text"]).get("success")
    except (json.JSONDecodeError, TypeError):
        success = False

    if not success:
        error = f"Login failed: {response['text']}."
        logger.warning(error + " " + instance["fqdn"])

    instance["juice_login"] = parse_response(response=response, elapsed=timer.elapsed)
    if not success or int(instance.get("version", 0)) < 7:
        return instance

    # logout of Juice
    if response and not response["error"]:
        response = await request(
            url=instance["juice_logout_url"],
            method="GET",
            session=session,
            timer=timer,
            semaphore=semaphore,
        )
        if response["error"] or response["reason"] != "OK":
            logger.error(
                f"LOGOUT {success}; {instance['fqdn']}: "
                f"reason: {response['reason']}, exception: {response['error']}"
            )

    return instance


async def bench_instances(
    loop: BaseSelectorEventLoop, instances: typing.List[dict]
) -> tuple[set[Task[dict]], set[Task[dict]]]:
    """Triggers the tasks for instances"""
    semaphore = asyncio.Semaphore(SEMAPHORE)

    connector = aiohttp.TCPConnector(
        limit=AIOHTTP_LIMITS, limit_per_host=AIOHTTP_LIMITS
    )
    session = aiohttp.ClientSession(
        connector_owner=True,
        loop=loop,
        raise_for_status=False,
        connector=connector,
    )
    tasks = await asyncio.wait(
        [
            loop.create_task(
                process_instance(
                    instance=instance, session=session, semaphore=semaphore
                )
            )
            for instance in instances
        ]
    )
    await connector.close()
    await session.close()
    return tasks


def generate_instance_urls(instance: dict) -> dict:
    """Generates URL from values of a given instance."""
    instance["juice_url"] = f"https://{instance['fqdn']}{JUICE_GET}"
    instance["juice_login_url"] = f"https://{instance['fqdn']}{JUICE_POST}"
    instance["juice_logout_url"] = f"https://{instance['fqdn']}{JUICE_LOGOUT}"
    instance["alto_url"] = f"https://{instance['fqdn']}{ALTO_GET}"

    instance["ayuda_bms_api_v2_url"] = f"https://{instance['fqdn']}{AYUDA_BMS_API_V2}"
    return instance


def start_bench(
    loop: typing.Union[asyncio.AbstractEventLoop, BaseSelectorEventLoop],
    instances: typing.List[dict],
) -> typing.List[dict]:
    """Starts the benching process"""
    tasks = bench_instances(
        loop=loop,
        instances=instances,
    )
    results = loop.run_until_complete(asyncio.gather(tasks))

    return [r.result() for r in results[0][0]]


def parse_response(
    response: typing.Union[dict, None],
    elapsed: typing.Union[int, None],
    exception: typing.Union[str, None] = None,
) -> dict:
    """Builds results dict from a response object, elapsed time and exception."""
    if response:
        return {
            "elapsed": elapsed,
            "code": response["status"],
            "reason": response["reason"],
            "exception": response["error"],
            "RowKey": padded_timestamp(),
        }
    else:
        return {
            "elapsed": elapsed,
            "code": None,
            "reason": None,
            "exception": exception,
            "RowKey": padded_timestamp(),
        }


def order_by_key(results: typing.List[dict], key: str) -> dict.values:
    """Splits a list or dict, based on a key"""
    per_cm_list = collections.defaultdict(list)
    for result in results:
        per_cm_list[result[key]].append(result)
    return per_cm_list.values()


def format_results_for_inserts(
    results: typing.List[dict],
) -> typing.Tuple[typing.List[dict], typing.List[dict], dict, dict]:
    """Takes the results and creates lists for the remaining tasks. Entities for
    inserting averages and individual results and to post back to Mamba."""
    averaged = []
    instances = []
    post_to_mamba = {}
    last_results = {}

    for cm in order_by_key(results, "cm"):
        juice, juice_login, alto, ayuda_bms_api_v2 = [0, 0, 0, 0]

        for instance in cm:
            last_results[instance["fqdn"]] = {}
            for app in ["juice", "juice_login", "alto", "ayuda_bms_api_v2"]:
                if not instance.get(app):
                    continue
                success = (
                    instance[app]["reason"] == "OK" and not instance[app]["exception"]
                )
                code = instance[app]["code"] or 0
                elapsed = instance[app]["elapsed"] or HTTP_TIMEOUT
                instances.append(
                    {
                        "PartitionKey": instance["fqdn"],
                        "RowKey": instance[app]["RowKey"],
                        "domain": instance["fqdn"].split(".", 1)[1],
                        "elapsed": elapsed,
                        "guid": instance["guid"],
                        "app": app,
                        "reason": instance[app]["reason"] or "Failed",
                        "code": code,
                        "success": success,
                    }
                )

                if app in ["juice", "juice_login"]:
                    app_dict = {
                        "app": app,
                        "code": instance[app]["code"] or 0,
                        "reason": instance[app]["reason"] or "Failed",
                        "elapsed": instance[app]["elapsed"] or HTTP_TIMEOUT,
                    }
                    last_results[instance["fqdn"]][app] = success

                    if not post_to_mamba.get(instance["guid"]):
                        post_to_mamba[instance["guid"]] = [app_dict]
                    else:
                        post_to_mamba[instance["guid"]].append(app_dict)

            juice += instance["juice"]["elapsed"] or HTTP_TIMEOUT
            juice_login += instance["juice_login"]["elapsed"] or HTTP_TIMEOUT
            alto += instance["alto"]["elapsed"] or HTTP_TIMEOUT
            if instance.get("ayuda_bms_api_v2"):
                ayuda_bms_api_v2 += (
                    instance["ayuda_bms_api_v2"]["elapsed"] or HTTP_TIMEOUT
                )

        total_instances = len(cm)
        raw_entities = (
            ("juice", juice / total_instances),
            ("juice_login", juice_login / total_instances),
            ("alto", alto / total_instances),
            ("ayuda_bms_api_v2", ayuda_bms_api_v2 / total_instances),
        )

        cloudmanager = cm[0]["cm"]
        for entity in [
            {
                "PartitionKey": cloudmanager,
                "RowKey": padded_timestamp(),
                "elapsed": avg[1],
                "app": avg[0],
            }
            for avg in raw_entities
        ]:
            averaged.append(entity)

    return averaged, instances, post_to_mamba, last_results


def padded_timestamp() -> str:
    """Returns an epoch timestamp, zero padded to 7 digit."""
    ts = str(time.time())
    if len(ts.split(".")[1]) != 7:
        fractions = ts.split(".")[1] + "0000000"
        ts = f"{ts.split('.')[0]}.{fractions[:7]}"
    return ts


def run_bench(instances: typing.List[dict]) -> typing.List[dict]:
    """Initiate the benching process"""

    current_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(current_loop)

    batch_results = start_bench(loop=current_loop, instances=instances)
    current_loop.close()
    return batch_results


async def run_inserts(
    avgs: typing.List[dict], instance_results: typing.List[dict], last_results: dict
):
    async with TableClient.from_connection_string(
        conn_str=os.environ["STORAGE_CONNECTION_STRING"],
        table_name=WEB_BENCH_RESULTS_AVG,
    ) as tc:
        for avg in sort_entities_by_pk(avgs):
            for entity in avg:
                try:
                    await tc.create_entity(entity)
                except ResourceNotFoundError:
                    async with TableServiceClient.from_connection_string(
                        conn_str=os.environ["STORAGE_CONNECTION_STRING"]
                    ) as ts:
                        await ts.create_table(WEB_BENCH_RESULTS_AVG)
                        await tc.create_entity(entity)

    async with TableClient.from_connection_string(
        conn_str=os.environ["STORAGE_CONNECTION_STRING"], table_name=WEB_BENCH_RESULTS
    ) as tc:
        for results in sort_entities_by_pk(instance_results):
            for entity in results:
                try:
                    await tc.create_entity(entity)
                except ResourceNotFoundError:
                    async with TableServiceClient.from_connection_string(
                        conn_str=os.environ["STORAGE_CONNECTION_STRING"]
                    ) as ts:
                        await ts.create_table(WEB_BENCH_RESULTS)
                        await tc.create_entity(entity)

    async with TableClient.from_connection_string(
        conn_str=os.environ["STORAGE_CONNECTION_STRING"],
        table_name=WEB_BENCH_RESULTS_LATEST,
    ) as tc:
        entity = {
            "PartitionKey": os.environ["REGION"],
            "RowKey": "latest",
            "results": json.dumps(last_results),
        }
        try:
            await tc.upsert_entity(entity)
        except ResourceNotFoundError:
            async with TableServiceClient.from_connection_string(
                conn_str=os.environ["STORAGE_CONNECTION_STRING"]
            ) as ts:
                await ts.create_table(WEB_BENCH_RESULTS_LATEST)
                await tc.upsert_entity(entity)


def sort_entities_by_pk(entities: typing.List[dict]) -> typing.List[dict.values]:
    result = collections.defaultdict(list)
    for entity in entities:
        result[entity["PartitionKey"]].append(entity)
    return list(result.values())


def list_chunker(entities: list, chunk_size: int) -> list:
    """Yield successive n-sized chunks from entities."""
    for i in range(0, len(entities), chunk_size):
        yield entities[i : i + chunk_size]
