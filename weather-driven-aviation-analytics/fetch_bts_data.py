import requests
from bs4 import BeautifulSoup
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

months = ['january','february','march','april','may','june','july',
	          'august','september','october','november','december']
default_file_name='T_ONTIME_REPORTING.csv'

required_params=[
    "FL_DATE", "ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID", "DEST_CITY_MARKET_ID", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "CANCELLED", "CANCELLATION_CODE"
]
dimension_data={
        "AIRPORT_ID": "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVecbeg_VQ",
        "AIRPORT_SEQ_ID": "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVecbeg_fRd_VQ",
        "CITY_MARKET_ID": "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_PVgl_ZNeXRg_VQ",
        "AIRPORT":"https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVecbeg",
        "CANCELLATION_CODE":"https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_PNaPRYYNgVba"

    }
tmpDir=f"{os.getcwd()}/tmp"
if not os.path.exists(tmpDir):
    os.makedirs(tmpDir)


def fetch_data_yearly(target_year, out_dir):
    """Downloads all months for a year. Idempotent — skips already-downloaded files.
    If interrupted via notebook Cancel, just re-run; completed downloads are kept."""
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(fetch_data, target_year, month, out_dir)
            for month in range(1, 13)
            if not check_file_exists(out_dir, f"{get_file_name(target_year, month)}.csv")
        ]
        for future in futures:
            future.result()
    #downloading dimension data
    fetch_dimension_data(out_dir)

def get_file_name(target_year, target_month):
    return f"airline_data_{target_year}_{months[target_month-1]}"

def fetch_data(target_year, target_month, out_dir):
    url = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"

    # Start a session to handle cookies automatically
    session = requests.Session()

    # 1. GET the page first to retrieve hidden ASP.NET form fields
    response = session.get(url, timeout=30)
    soup = BeautifulSoup(response.text, 'html.parser')

    def get_val(id):
        tag = soup.find('input', id=id)
        return tag['value'] if tag else ""

    # 2. Build the payload, in devtools its showing as form
    payload = {
        "__VIEWSTATE": get_val("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": get_val("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": get_val("__EVENTVALIDATION"),
        "cboYear": str(target_year),
        "cboPeriod": str(target_month), #it is 1 indexed
        "cboGeography": "All",
        "btnDownload": "Download",
    }

    add_required_fields(payload)
    
    print(f"Requesting data for {target_year} {months[target_month-1]}...")
    
    # 3. POST the request to trigger the download
    # Stream=True is used because the response is a ZIP file
    tmp_file = f"{tmpDir}/{get_file_name(target_year, target_month)}.zip"
    try:
        with session.post(url, data=payload, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(f"{tmp_file}", 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Successfully fetched for {target_year} {months[target_month-1]}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {target_year} {months[target_month-1]}: {e}")
        return
    try:
        out_file=f"{out_dir}/{get_file_name(target_year, target_month)}.csv"
        extract_file(tmp_file, out_file)
        os.remove(tmp_file)
    except Exception as e:
        print(f"Error extracting {tmp_file}: {e}")

def check_file_exists(out_dir, filename):
    return os.path.exists(os.path.join(out_dir, filename))

def extract_file(file, out_file):
    with zipfile.ZipFile(file, 'r') as zip_ref:
        info = zip_ref.getinfo(default_file_name)
        # Change the filename attribute in memory
        info.filename = f"{out_file.split('/')[-1]}"
        # Extract using the modified ZipInfo object
        zip_ref.extract(info, path=os.path.dirname(out_file))

def add_required_fields(payload):
    for param in required_params:
        payload[param] = "on"


def fetch_dimension_data(out_path):
    static_dir = f"{out_path}/dimension_data"
    os.makedirs(static_dir, exist_ok=True)
    session = requests.Session()
    for key, url in dimension_data.items():
        filename = f"{key}.csv"
        full_path = f"{static_dir}/{filename}"
        if check_file_exists(static_dir, filename):
            continue
        try:
            response = session.get(url, timeout=60)
            response.raise_for_status()
            with open(full_path, 'wb') as f:
                f.write(response.content)
            print(f"Successfully fetched {filename}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {filename}: {e}")

if __name__=="__main__":
    fetch_data(2023, 1, f"{os.getcwd()}/tmp")
