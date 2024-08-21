#### ------------libraries import------------ ####

import streamlit as st
from bs4 import BeautifulSoup
import requests
import re
import numpy as np
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
    # MainMenu {visibility: hidden;}
    # footer {visibility: hidden;}
    header {visibility: hidden;}
    </style> 
    """
st.markdown(hide_st_style, unsafe_allow_html = True)

#### -----------function definition---------- ####

def cleanText(text):
    newPunc = ''.join(set('!#\*/;@[\\]^_`{|}~') - {'.'})
    newText = text.translate(str.maketrans('', '', newPunc))  # Remove unnecessary punctuation
    newText = ' '.join(newText.split())  # Keep only one white space
    return newText

def summ(scraped):
    pass


def get_text(url):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(random.uniform(1,5))
    page_text = driver.find_element(By.TAG_NAME, "body").text
    soup = BeautifulSoup(page_text, 'html.parser')
    driver.quit()
    return soup.get_text()
            
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
        # st.write("Keyword pattern", patterns)
    except Exception as e:
        st.error(f"An error occurred while reading the file: {e}")
else:
    st.warning(":receipt: uploading file to continue!")

#### --------uploaded data preparation-------- ####

if uploaded_file is not None:
    unique_set = patterns['set'].unique()
    for set_name in unique_set:
        pattern_dict = dict(zip(patterns[patterns['set'] == set_name]['topic'], patterns[patterns['set'] == set_name]['pattern']))
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

        mu, sigma = 1, 0.1 # mean and standard deviation
        s = np.random.normal(mu, sigma, 1000)
        
        scrape = get_text(rl)
        if scrape:
            ws.append([scrape])
        else:
            ws.append([""])
            
        timestamp.append(current_datetime)
        time.sleep(random.choice(s))
        progress_bar.progress(int((idx + 1) * progress_step))
            
    cleaned = []
    for web in ws:
        for text in web:
            clean = cleanText(str(text))
            cleaned.append(clean)

    url['Manual-Fact-Sale Sheet'] = pdf

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
        fi_type = row['FI_type']
        product_type = row['Product_type']
        scraped = row['scraped']
        product = row['Product_Name']
        type = row['Type']
        u_rl = row['URL']
        pdf = row['Manual-Fact-Sale Sheet']
        timestamp = row['timestamp']
        clean_ws = cleanText(scraped)

        if product not in data:
            data[product] = {
                "Group": group,
                "Abbreviation": bank_abb,
                "FI": bank_name,
                "FI_type": fi_type,
                "Type" : type,
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
                "Type" : details["Type"],
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
