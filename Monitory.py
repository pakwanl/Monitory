#### ------------libraries import------------ ####

import streamlit as st
from bs4 import BeautifulSoup
import requests
import re
import random
import pandas as pd
import time
from urllib.parse import urljoin
import datetime
import pytz
import xlsxwriter
from io import BytesIO
import subprocess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

#### ----------------setting----------------- ####

hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style> 
    """
st.markdown(hide_st_style, unsafe_allow_html = True)

#### -------------Tableau connect------------ ####

    ## Disabled Tableau Function (SSLErrors)

#### -----------function definition---------- ####

def cleanText(text):
    newPunc = ''.join(set('!#\*/;@[\\]^_`{|}~') - {'.'})
    newText = text.translate(str.maketrans('', '', newPunc))  # Remove unnecessary punctuation
    newText = ' '.join(newText.split())  # Keep only one white space
    return newText

def get_text_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup.get_text()
    except requests.exceptions.RequestException as e:
        return f"Request error for URL '{url}': \n{e}"
    except requests.exceptions.SSLError as e:
        return f"SSL error for URL '{url}': \n{e}"

def get_text_java(url):
    options = Options()
    options.add_argument('--headless')

    # Ensure webdriver_manager is installed and up to date
    try:
        result = subprocess.run(["pip", "show", "webdriver-manager"], capture_output=True, text=True)
        if "webdriver-manager" not in result.stdout:
            print("Installing webdriver-manager...")
            subprocess.run(["pip", "install", "webdriver-manager"])
        else:
            current_version = result.stdout.split("\n")[1].split(": ")[-1]
            if current_version < "0.8.3":
                print(f"Upgrading webdriver_manager (current: {current_version})...")
                subprocess.run(["pip", "install", "--upgrade", "webdriver-manager"])
    except subprocess.CalledProcessError as e:
        print(f"Error checking/upgrading webdriver_manager: {e}")

    # Ensure Chrome is installed
    try:
        result = subprocess.run(["which", "google-chrome"], capture_output=True, text=True)
        if not result.stdout.strip():
            print("Chrome not found. Please install Chrome or set the appropriate environment variable.")
            return ""
    except subprocess.CalledProcessError:
        print("Error checking Chrome installation. Please ensure Chrome is installed.")
        return ""

    try:
        driver_path = ChromeDriverManager().install()
        driver = webdriver.Chrome(service=Service(driver_path), options=options)
        driver.get(url)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        return soup.get_text()
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        if 'driver' in locals():
            driver.quit()
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Output')
    workbook = writer.book
    worksheet = writer.sheets['Output']  
    writer.close()
    processed_data = output.getvalue()
    return processed_data
    
def get_pdf(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')
        pdf_urls = []
        for link in links:
            href = link.get('href')
            if href and href.lower().endswith(('.pdf', '.pdf.aspx')): # <- add more type of file available here
                download_url = urljoin(url, href)
                pdf_urls.append(download_url)
        return pdf_urls
    except requests.exceptions.RequestException as e:
        return [f"{e}"]
    except requests.exceptions.SSLError as e:
        return [f"{e}"]

def extract_matches(text, patterns):
    matches = {}
    for criterion, pattern in patterns.items():
        matches[criterion] = []
        for match in re.finditer(pattern, text):
            match_value = match.group()
            if len(match_value) <= 900:
                matches[criterion].append(match_value)
    return matches

#### ---------------data import--------------- ####

uploaded_file = st.file_uploader("Upload here :lightning_cloud:", type=None, accept_multiple_files=False)
if uploaded_file is not None:
    try:
        url = pd.read_excel(uploaded_file, sheet_name="product")
        url = pd.DataFrame(url)

        patterns = pd.read_excel(uploaded_file, sheet_name="pattern")
        patterns = pd.DataFrame(patterns)

        st.write("Product", url)
        st.write("Keyword pattern", patterns)
    except Exception as e:
        st.error(f"An error occurred while reading the file: {e}")
else:
    st.warning(":receipt: uploading file to continue!")

#### --------uploaded data preparation-------- ####

if uploaded_file is not None:
    unique_set = patterns['set'].unique()
    for set_name in unique_set:
        pattern_dict = dict(zip(patterns[patterns['set'] == set_name]['mc'], patterns[patterns['set'] == set_name]['pattern']))
        globals()[set_name] = {key: fr"{value}" for key, value in pattern_dict.items()}
        
    url = url[url.Status == 'keep']
    url = url.reset_index(drop=True)

#### --------------web scraping-------------- ####

def scrape_data(url, unique_set):
    ws = []
    timestamp = []
    pdf = []

    progress_bar = st.progress(0)
    total_urls = len(url)
    progress_step = 100 / total_urls if total_urls > 0 else 0

    for idx, rl in enumerate(url['URL']):
        current_datetime = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%d %H:%M:%S")
        pdf_files = get_pdf(rl)
        pdf.append('\n\n- '.join(pdf_files) if pdf_files else 'x')
        min_sleep = 1
        max_sleep = 5
        sleep_time = random.uniform(min_sleep, max_sleep)
        time.sleep(sleep_time)
        if url.loc[idx, 'scrapable'] == 'Java':
            scrape = get_text_java(rl)
            if scrape:
                ws.append([scrape])
            else:
                ws.append([""])
            timestamp.append(current_datetime)
            time.sleep(sleep_time)
            progress_bar.progress(int((idx + 1) * progress_step))
        else:
            scrape = get_text_html(rl)
            if scrape:
                ws.append([scrape])
            else:
                ws.append([""])
            timestamp.append(current_datetime)
            time.sleep(sleep_time)
            progress_bar.progress(int((idx + 1) * progress_step))
            
    cleaned = []
    for web in ws:
        for text in web:
            clean = cleanText(str(text))
            cleaned.append(clean)

    url['Manual-Fact-Sale Sheet'] = pdf
    
    # qc = []
    # for row in url['Manual-Fact-Sale Sheet']:
    #     if row == 'x':
    #         qc.append('FALSE')
    #     else:
    #         qc.append('TRUE')

    url['timestamp'] = timestamp
    url['scraped'] = cleaned
    # url['QC'] = qc

    url_ = url[url.Status != 'x']
    url_ = url_.reset_index(drop=True)

    data = {}
    for index, row in url_.iterrows():
        group = row['Group']
        bank_abb = row['Bank_abb']
        bank_name = row['Bank_name']
        type_ = row['type']
        product_type = row['Product_type']
        scraped = row['scraped']
        product = row['Product_Name']
        status = row['Status']
        u_rl = row['URL']
        pdf = row['Manual-Fact-Sale Sheet']
        timestamp = row['timestamp']
        clean_ws = cleanText(scraped)

        if product not in data:
            data[product] = {
                "Group": group,
                "Abbreviation": bank_abb,
                "FI": bank_name,
                "FI_type": type_,
                "Status" : status,
                "product_type": product_type,
                "URL": u_rl,
                "PDF": pdf,
                "timestamp": timestamp,
                "keywords": []
            }

        for set_name in unique_set:
            matched = extract_matches(clean_ws, globals()[set_name])  # iterate over keyword set
            for key in matched.keys():
                count = len(matched[key])
                if count != 0:
                    sentence_info = f"พบข้อความ"
                else:
                    sentence_info = "ไม่พบข้อความ"

                data[product]["keywords"].append({
                    "keyword_set": set_name,
                    "keyword": key,
                    "Sentences_found": sentence_info,
                    "Sentences": '\n\n- '.join(matched[key]) if matched[key] else f"ไม่พบข้อความเกี่ยวกับ '{key}'"
                })

    flattened = []
    for product, details in data.items():
        for keyword_info in details["keywords"]:
            flattened.append({
                "Group": details["Group"],
                "Abbreviation": details["Abbreviation"],
                "FI": details["FI"],
                "FI_type": details["FI_type"],
                "Product": product,
                "Product_type": details["product_type"],
                "URL": details["URL"],
                "PDF": details["PDF"],
                "Status" : details["Status"],
                "timestamp": details["timestamp"],
                "Keyword_Set": keyword_info["keyword_set"],
                "keyword": keyword_info["keyword"],
                "Sentences_found": keyword_info["Sentences_found"],
                "Sentences": keyword_info["Sentences"]
            })

    return pd.DataFrame(flattened)

if uploaded_file is not None and st.button("Start Scraping!"):
    scraped_data = scrape_data(url, unique_set)
    st.session_state['scraped_data'] = scraped_data

#### -----------filter and display------------ ####

if 'scraped_data' in st.session_state:
    
    scraped_data = st.session_state['scraped_data']
    group_filter = st.multiselect("Select Group", options=scraped_data["Group"].unique(), default=scraped_data["Group"].unique())
    filtered_group = scraped_data[scraped_data["Group"].isin(group_filter)]
    
    fi_filter = st.multiselect("Select FI", options=filtered_group["FI"].unique(), default=filtered_group["FI"].unique())
    filtered_fi = filtered_group[filtered_group["FI"].isin(fi_filter)]
    
    product_type_filter = st.multiselect("Select product type", options=filtered_fi["Product_type"].unique(), default=filtered_fi["Product_type"].unique())
    filtered_type_product = filtered_fi[filtered_fi["Product_type"].isin(product_type_filter)]

    product_filter = st.multiselect("Select product", options=filtered_type_product["Product"].unique(), default=filtered_type_product["Product"].unique())
    filtered_product = filtered_type_product[filtered_type_product["Product"].isin(product_filter)]
    
    keyword_set_filter = st.multiselect("Select pattern set", options=filtered_product["Keyword_Set"].unique(),default=filtered_product["Keyword_Set"].unique())
    filtered_keyword = filtered_product[filtered_product["Keyword_Set"].isin(keyword_set_filter)]
    
    filtered_data = scraped_data[
        (scraped_data["Group"].isin(group_filter)) &
        (scraped_data["FI"].isin(fi_filter)) &
        (scraped_data["Keyword_Set"].isin(keyword_set_filter))&
        (scraped_data["Product_type"].isin(product_type_filter))&
        (scraped_data["Product"].isin(product_filter))
    ]

    current_datetime = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%d")
    st.write(":sparkler: Filtered Information :sparkler:")
    st.write(filtered_data)
    df_xlsx = pd.DataFrame(scraped_data)
    xlsx = to_excel(df_xlsx)
    st.download_button(label='📥 Download output',
                                    data=xlsx ,
                                    file_name= f"output_{current_datetime}.xlsx")
