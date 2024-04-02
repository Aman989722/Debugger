# main.py
# ToDo (next release)
# handle duplicate requests
# handle urls with query string appended

# Initial Setup
start_page = "https://www.sbicard.com/"
# not setting page scan limit will throw error
limit = 1
# use below list to exclude specific diretory/pages
exclude_list = ["mailto:", "javascript:", "#", "account", "result"]
# Setup complete

from read_excels import excelWorkbook
import os
import sys

try:
    os.system(f"{sys.executable} -m pip install pyppdf")
    os.system(f"{sys.executable} -m pip install pyppeteer")
    os.system(f"{sys.executable} -m pip install pandas")
except Exception as e:
    print("Modules not installed")

from urllib.parse import urlparse, parse_qs
import asyncio
from pyppeteer import launch, launcher
from pyppeteer.network_manager import Request, Response
import pyppeteer
import urllib.parse
import traceback
import csv
import time
import pandas as pd
import pyppdf.patch_pyppeteer
import requests
from bs4 import BeautifulSoup
import zipfile


# Modified Imports - 2023-11-16, Mishra Aman
# Asyncio patch for nested entrants
# os.system(f'{sys.executable} -m pip install nest_asyncio')
# import nest_asyncio
# nest_asyncio.apply()

# Check script execution time
start_time = time.time()
global_df = pd.DataFrame()
global_df["Base URL"] = None
global_df["Query Parameters"] = None
tracking_server_value = None
tracking_server_string = "Frst Party"
# from Crawler
master_hrefs = []
# base domain can come from input or api call parameters
base_domain = (
    start_page.replace("http://", "").replace("https://", "").replace("www.", "")
)
base_domain = base_domain.split("/")[0]
# set up input for user to add paths, directories to exclude from crawling
# exclude list will work as string match only


def ensure_correct_url_format(url: str) -> str:
    """
    Ensure the URL starts with either 'http://' or 'https://'
    """
    if url.startswith("//"):
        url = "http:" + url  # or 'https:' based on your requirement
    elif not url.startswith(("http://", "https://")):
        url = "http://" + url.lstrip("/")
    return url


# This will return True/False checking hrefs - True means add it to _indexer else (False) exclude
def _excludeCheck(_h):
    _h = _h.lower()
    _ret = True  # setting true as default value
    for _i in exclude_list:
        _i = _i.lower()
        if _h.find(_i) == -1:
            # not in exclude list (add it)
            _ret = True
        else:
            # let us excoude this href
            _ret = False
            return _ret
    return _ret


if start_page[len(start_page) - 1] == "/":
    pass
else:
    start_page = str(start_page) + "/"

_obj = {}
_obj["page_url"] = start_page
_obj["crawlStatus"] = "pending"

master_hrefs.append(_obj)


def get_protocol(url):
    if "https:" in url:
        p = "https://"
    elif "http:" in url:
        p = "http://"
    else:
        p = "http://"
    return p


def add_url_data_to_df(df, parsed_dict):
    # Initialize a list to store new data
    new_data = []

    string_col_name = f"Base URL {parsed_dict['base_url']}\n"
    path_data = f"{string_col_name} Paths:"
    for path in parsed_dict["paths"]:
        path_data = f"{path_data} {path}"
    # Process Base URL
    new_data.append({"URL Component": "Base URL", "Value": path_data})

    # Process Query Parameters
    for key, value in parsed_dict["query_parameters"].items():
        formatted_param = "{}: {}".format(key, value[0] if value else "")
        new_data.append({"URL Component": "Query Parameter", "Value": formatted_param})

    # Convert new data to a DataFrame
    new_rows_df = pd.DataFrame(new_data)
    global global_df
    # Concatenate the new DataFrame to the global DataFrame
    global_df = pd.concat([df, new_rows_df], ignore_index=True)


def format_parsed_url(parsed_dict):
    result = []
    # Base URL
    result.append("Base URL: " + parsed_dict["base_url"])
    result.append("\nPaths:")
    for path in parsed_dict["paths"]:
        result.append(path)
    # Query Parameters
    result.append("\nQuery Parameters:")

    for k, v in parsed_dict["query_parameters"].items():
        result.append("{}: {}".format(k, v[0] if v else ""))

    return "\n".join(result)


def make_folder(base_domain):
    _dir = base_domain
    _time = time.asctime().replace(":", "-").replace(" ", "_").lower()
    _path = os.getcwd() + str("/" + _dir + "_" + _time)
    os.mkdir(_path)
    _file_info = {}
    _file_info["complete_path"] = _path
    _file_info["folder_name"] = str(_dir + "_" + _time)
    return _file_info


# Create new folder for this run
file_info = make_folder(base_domain)


def create_csv(file_name, li_to_add, headers):
    access_to_file = file_info["folder_name"] + "/" + file_name

    if (os.path.exists(access_to_file)) == True:
        # don't set header
        with open(access_to_file, "a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(li_to_add)
    else:
        # write headers
        with open(access_to_file, "a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerow(li_to_add)


def _cookieWriter(file_name, li_to_add):
    headers = [
        "Page URL",
        "Page Title",
        "Status Code",
        "Cookie Name",
        "Cookie Value",
        "Domain",
        "Path",
        "Expiry",
        "Size",
        "httpOnly",
        "Secure",
        "Session",
    ]
    access_to_file = file_info["folder_name"] + "/" + file_name

    if (os.path.exists(access_to_file)) == True:
        # don't set header
        with open(access_to_file, "a", newline="") as file:
            writer = csv.writer(file)
            for _c in li_to_add:
                _arr = []
                _arr.append(current_page_details.get("url", ""))
                _arr.append(current_page_details.get("title", ""))
                _arr.append(current_page_details.get("statusCode", "200"))
                _arr.extend(list(_c.values()))
                writer.writerow(_arr)
    else:
        # write headers
        with open(access_to_file, "a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            for _c in li_to_add:
                _arr = []
                _arr.append(current_page_details.get("url", ""))
                _arr.append(current_page_details.get("title", ""))
                _arr.append(current_page_details.get("statusCode", "200"))
                _arr.extend(list(_c.values()))
                writer.writerow(_arr)


def parse_beacon(url):
    parsed_url = urlparse(url)

    # Extract base URL
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    # Split paths
    paths = parsed_url.path.split("/")[1:]

    # Parse query parameters
    query_parameters = parse_qs(parsed_url.query)

    return {"base_url": base_url, "paths": paths, "query_parameters": query_parameters}


def _indexer(_arr, current_domain):
    protocol = get_protocol(current_domain)
    current_domain = (
        current_domain.replace("http://", "")
        .replace("https://", "")
        .replace("www.", "")
    )
    current_domain = current_domain.split("/")[0]
    if len(_arr) > 0:
        for val in _arr:
            try:
                _href = val.attrs["href"]
                if (
                    (base_domain in _href)
                    and (_href not in str(master_hrefs))
                    and (_excludeCheck(_href))
                ):
                    _obj = {}
                    _obj["page_url"] = urllib.parse.unquote(_href)
                    _obj["crawlStatus"] = "pending"
                    master_hrefs.append(_obj)
                elif (
                    _href[0] == "/"
                    and (
                        str(protocol) + (current_domain + _href).replace("//", "/")
                        not in str(master_hrefs)
                    )
                    and (_excludeCheck(_href))
                ):
                    _obj = {}
                    _obj["page_url"] = urllib.parse.unquote(
                        str(protocol) + (current_domain + _href).replace("//", "/")
                    )
                    _obj["crawlStatus"] = "pending"
                    master_hrefs.append(_obj)
            except:
                pass


def _crawler(url):
    try:
        r = requests.get(ensure_correct_url_format(url))
        statusCode = r.status_code
        print()

        if statusCode == (200 or 400):
            content = r.content
            soup = BeautifulSoup(content, "lxml")
            # allScriptsList = soup.find_all('script')
            allAnchorTags = soup.find_all("a")
            pageTitle = soup.find("title")
            pageTitle = pageTitle.string
            launchScript = soup.select(selector='head script[src*="launch"]')

            # indexer function will be called here on allAnchorTags
            _indexer(allAnchorTags, str(url))
            # format file
            global current_page_details
            current_page_details = {}
            current_page_details["url"] = url
            current_page_details["statusCode"] = statusCode
            current_page_details["title"] = pageTitle
            current_page_details["launchScript"] = "|".join(
                [ref.get("src", "") for ref in launchScript]
            )
            current_page_details["title"] = pageTitle
            current_page_details["time_load"] = r.elapsed.total_seconds()

            print(current_page_details)
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        print("Unexpected error occured\nPlease retart program")
        quit()


# Create zip file for data extracted
def _zipFile(file_name):
    file_name = str(file_name)
    zf = zipfile.ZipFile(str(file_name) + ".zip", "w")
    for dirname, subdirs, files in os.walk(file_name):
        zf.write(dirname)
        for filename in files:
            zf.write(os.path.join(dirname, filename))
    zf.close()


# end --------------------------
async def intercept_request(req: Request):
    try:
        """
        if (req.resourceType == 'image') and (req.url.endswith('.png') or req.url.endswith('.jpg')):
            await req.abort()
        else:
            await req.continue_()
        """
        await req.continue_()
    except Exception as e:
        print(f"Error in intercept_request: {e}")


tokens = []
token_dict = {}
index = 0


async def intercept_response(res: Response):
    status = res.status
    url = res.url
    _req = res.request
    _reqURL = _req.url
    _reqMethod = _req.method

    # Get Adobe Analytics Calls
    # Custom link calls: not included
    if ("b/ss" in url) and (status == 200) and "pe=lnk_" not in url:
        aa_header_arr = [
            "Page URL",
            "Page Title",
            "Status Code",
            "Method",
            "Tracking Server",
            "Report Suite",
            "Version",
            "Size",
            "Request URL",
            "beacon",
            "Page load time",
            "Launch Script",
        ]
        _arr = []
        _arr.append(current_page_details.get("url", ""))
        _arr.append(current_page_details.get("title", ""))
        _arr.append(current_page_details.get("statusCode", status))
        _arr.append(_reqMethod)
        if _reqMethod == "GET":
            aa_payload = urllib.parse.unquote(res.url)
        elif _reqMethod == "POST":
            aa_payload = urllib.parse.unquote(_req.postData)

        trimmed_aa_payload = aa_payload.replace("https://", "").replace("http://", "")
        trimmed_aa_payload = trimmed_aa_payload.split("/")
        _arr.append(trimmed_aa_payload[0])
        global tracking_server_value
        tracking_server_value = trimmed_aa_payload[0]
        print(tracking_server_value)
        _arr.append(trimmed_aa_payload[3])
        _arr.append(trimmed_aa_payload[5])
        _arr.append(len(aa_payload))
        # print(aa_payload)
        _arr.append(aa_payload)
        print(aa_payload)
        print(format_parsed_url(parse_beacon(aa_payload)))
        _arr.append(format_parsed_url(parse_beacon(aa_payload)))
        # Added launch script src path
        add_url_data_to_df(global_df, parse_beacon(aa_payload))
        _arr.append(current_page_details.get("time_load", 0))
        _arr.append(current_page_details.get("launchScript", ""))
        global tokens, token_dict, index
        tokens.append(format_parsed_url(parse_beacon(aa_payload)))
        index += 1
        token_dict[f"header_{index}"] = format_parsed_url(parse_beacon(aa_payload))

        # write to file
        create_csv("adobe_analytics.csv", _arr, aa_header_arr)

    # Get Google Analytics Calls
    if (
        ("google-analytics.com/" in _reqURL)
        and (status == 200)
        and ("/collect" in _reqURL)
    ):
        ga_header_arr = [
            "Page URL",
            "Page Title",
            "Status Code",
            "Method",
            "Tracking Code",
            "Event",
            "Language",
            "Size",
            "Request URL",
        ]
        _arr = []
        _arr.append(current_page_details.get("url", ""))
        _arr.append(current_page_details.get("title", ""))
        _arr.append(current_page_details.get("statusCode", status))

        if _reqMethod == "GET":
            ga_payload = urllib.parse.unquote(res.url)
        elif _reqMethod == "POST":
            ga_payload = urllib.parse.unquote(_reqURL)

        trimmed_ga_payload = ga_payload.replace("https://", "").replace("http://", "")
        trimmed_ga_payload = trimmed_ga_payload.split("&")
        _arr.append(_reqMethod)
        _ix = [i for i, s in enumerate(trimmed_ga_payload) if "tid=UA-" in s]
        if (len(_ix)) > 0:
            _arr.append(trimmed_ga_payload[_ix[0]].replace("tid=", ""))
        else:
            _arr.append("Not Found")
        if "t=pageview" in ga_payload:
            _arr.append("pageview")
        else:
            _arr.append("Not Found")
        _arr.append(len(ga_payload))
        _arr.append(ga_payload)
        # write to file
        create_csv("google_analytics.csv", _arr, ga_header_arr)

    # Check Adobe Target
    if ("tt.omtrdc.net" in _reqURL) and status == 200:
        target_header_arr = [
            "Page URL",
            "Page Title",
            "Status Code",
            "Method",
            "Tracking Server",
            "Version",
            "Size",
            "Request URL",
            "postData",
            "Launch Script",
        ]
        _arr = []
        _arr.append(current_page_details.get("url", ""))
        _arr.append(current_page_details.get("title", ""))
        _arr.append(current_page_details.get("statusCode", status))
        _arr.append(_reqMethod)
        _arr.append(
            _reqURL.replace("https://", "").replace("http://", "").split("/")[0]
        )
        if _reqMethod == "GET":
            tt_payload = urllib.parse.unquote(res.url)
            tt_payload = tt_payload.lower()
            if "version" in tt_payload:
                str1 = tt_payload.split("version=")[1]
                str2 = str1.split("&")[0]
                _arr.append(str2)
            else:
                _arr.append("")
            _arr.append(len(tt_payload))
            _arr.append(tt_payload)
            _arr.append("GET Request")

        elif _reqMethod == "POST":
            _arr.append(_reqURL.split("version=")[1])
            _arr.append(len(_reqURL))
            _arr.append(_reqURL)
            _arr.append(_req.postData)
            tt_payload = urllib.parse.unquote(_req.postData)
        # write to file
        _arr.append(current_page_details.get("launchScript", ""))
        create_csv("adobe_target.csv", _arr, target_header_arr)
    # Check Decibel Code
    if (
        ("cdn.decibelinsight.net/" in _reqURL)
        and (status == 200)
        and (_reqMethod == "GET")
        and ("/di.js" in _reqURL)
    ):
        decibel_payload = urllib.parse.unquote(_reqURL)
        decibel_header_arr = [
            "Page URL",
            "Page Title",
            "Status Code",
            "Method",
            "accountNumber",
            "da_websiteId",
            "Size",
            "Request URL",
        ]
        _arr = []
        _arr.append(current_page_details.get("url", ""))
        _arr.append(current_page_details.get("title", ""))
        _arr.append(current_page_details.get("statusCode", status))
        _arr.append(_reqMethod)
        trimmed_decibel_payload = (
            decibel_payload.replace("https://", "").replace("http://", "").split("/")
        )
        _arr.append(trimmed_decibel_payload[2])
        _arr.append(trimmed_decibel_payload[3])
        _arr.append(len(decibel_payload))
        _arr.append(decibel_payload)
        # write to file
        create_csv("decibel_insight.csv", _arr, decibel_header_arr)
    pass


async def main(pageURL):
    print("..main..")

    try:
        # Browser startup parameters

        start_params = {
            # Close the headless browser (The default is to start headless)
            "headless": True,
            # Ignore HTTPS errors
            "ignoreHTTPSErrors": True,
            # Enable dumping IO
            "dumpio": True,
        }

        # Use the bundled Chromium with Pyppeteer
        browser = await pyppeteer.launch(
            # executablePath=pyppeteer.chromium_downloader.chromiumExecutable.get('linux'),
            # headless=True,
            args=["--no-sandbox"],
            **start_params,  # Pass the startup parameters here
        )
        # Create a page object, page operations are performed on the object
        page = await browser.newPage()

        # JS is true by default
        # await page.setJavaScriptEnabled(enabled=True)

        await page.setRequestInterception(True)

        page.on("request", lambda req: asyncio.ensure_future(intercept_request(req)))
        page.on("response", lambda res: asyncio.ensure_future(intercept_response(res)))
        await page.goto(pageURL, {"timeout": 60000})
        _cookies = await page.cookies()
        _cookieWriter("cookies.csv", _cookies)
        cur_dist = 0
        height = await page.evaluate("() => document.body.scrollHeight")
        while True:
            if cur_dist < height:
                await page.evaluate("window.scrollBy(0, 500);")
                await asyncio.sleep(0.2)
                cur_dist += 500
            else:
                break
        await browser.close()

    except Exception as e:
        print("Exception occurred:")
        traceback.print_exc()
        pass


def write_to_file(file_name, data):
    print(len(tokens))
    access_to_file = file_info["folder_name"] + "/" + file_name
    with open(access_to_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(tokens)


def write_to_dict(file_name, data_dict):
    access_to_file = file_info["folder_name"] + "/" + file_name
    with open(access_to_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data_dict.keys())
        writer.writeheader()
        writer.writerow(data_dict)


for _index, _val in enumerate(master_hrefs):
    if _val["crawlStatus"] == "pending" and _index < int(limit):
        print("---------------------")
        print("Urls Found: ", len(master_hrefs))
        print("---------------------\n")
        _crawler(_val["page_url"])
        asyncio.get_event_loop().run_until_complete(main(_val["page_url"]))
        _val["crawlStatus"] = "done"
    else:
        print("quit")
        end_time = time.time()
        time_taken = str(end_time - start_time)
        print(
            f"Program Compelete\nExecution Time: {time_taken} seconds for {_index} Web"
            " Pages."
        )

        write_to_file("adobe_analytics_no_header.csv", tokens)
        write_to_dict("adobe_analytics_header.csv", token_dict)
        if tracking_server_value is not None and tracking_server_value.endswith(base_domain):
            tracking_server_string = "First party"
        else:
            tracking_server_string = "Third party"
        # Adobe analytics file
        try:
            obj = excelWorkbook("adobe_analytics")
            if obj.create_excel():
                obj.worksheet_formatting(7)
                obj.add_worksheet("Summary",tracking_server_string)
                obj.add_parsed_beacon_data(
                    beacon_column="beacon"
                )  # Add the parsed beacon data as a new sheet
                print("adding")

            # Delete csv files
            os.remove(obj.my_path.joinpath(obj.file_name + ".csv"))

        except Exception as E:
            print("Exception occured due to : ", E)

        # Target file
        try:
            obj = excelWorkbook("adobe_target")
            if obj.create_excel():
                obj.worksheet_formatting(6)
            # Delete csv files
            os.remove(obj.my_path.joinpath(obj.file_name + ".csv"))

        except Exception as E:
            print("Exception occured due to : ", E)

        # create zip file
        _zipFile(file_info.get("folder_name"))
        print(global_df)
        global_df.to_csv("results.csv")
        quit()
