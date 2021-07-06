import logging
import traceback

import asyncio
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://api.crossref.org/works"


def process(path):
    url = f"{BASE_URL}/{path}"
    response = None
    status = "SUCCESS"

    logging.info(f"Processing path {url}")

    try:
        response = requests.get(url=url, verify=False)
        response = response.json()
    except:
        status = "ERROR"
        logging.error(f"Error to process url {url}", traceback.print_exc())

    return {
        "status": status,
        "path": path,
        "response": response
    }


def clean_results(results):
    logging.info("Cleaning results...")


def save(results_cleaned):
    logging.info("Saving")


async def proccess_async(paths):
    results = []

    logging.info("{0:<30} {1:>20}".format("File", "Completed at"))
    with ThreadPoolExecutor(max_workers=15) as executor:
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(
            executor, process, (path),) for path in paths]

        for result in await asyncio.gather(*tasks):
            status = result.get("status")
            path = result.get("path")
            logging.info(f"File {path} processed with {status}.")

            results.append(result)

    print(f"Qtd processed items: {len(results)}")

    # TO DO
    results_cleaned = clean_results(results)
    save(results_cleaned)


def get_doi_paths():
    return pd.read_parquet('data.parquet', columns=['doi'])['doi'].tolist()


def run():
    paths = get_doi_paths()

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(proccess_async(paths))
    loop.run_until_complete(future)


run()
