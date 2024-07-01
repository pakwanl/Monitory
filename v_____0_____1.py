#### ------------libraries import------------ ####

import streamlit as st
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from urllib.parse import urljoin
import datetime
import pytz  # Import for timezone conversion

#### -----------function definition---------- ####

def cleanText(text):
    newPunc = ''.join(set('!#\*/;@[\\]^_`{|}~') - {'.'})
    newText = text.translate(str.maketrans('', '', newPunc))  # Remove unnecessary punctuation
    newText = ' '.join(newText.split())  # Keep only one white space
    return newText

def get_all_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup.get_text()
    except requests.exceptions.RequestException as e:
        return f"Request error for URL '{url}': \n{e}"
    except requests.exceptions.SSLError as e:
        return f"SSL error for URL '{url}': \n{e}"

def get_pdf(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a')
        pdf_urls = []
        for link in links:
            href = link.get('href')
            if href and href.lower().endswith('.pdf'):
                download_url = urljoin(url, href)
                pdf_urls.append(download_url)
        return pdf_urls
    except requests.exceptions.RequestException as e:
        return []
    except requests.exceptions.SSLError as e:
        return []

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

uploaded_file = st.file_uploader("Upload info_pool.xlsx to continue :lightning_cloud:", type=None, accept_multiple_files=False)
if uploaded_file is not None:
    url = pd.read_excel(uploaded_file, sheet_name="product")
    url = pd.DataFrame(url)

    patterns = pd.read_excel(uploaded_file, sheet_name="pattern")
    patterns = pd.DataFrame(patterns)

    st.write("Product", url)
    st.write("Keyword pattern", patterns)
else:
    st.warning(":receipt: waiting for file to continue!")

#### --------uploaded data preparation-------- ####

if uploaded_file is not None:
    unique_set = patterns['set'].unique()
    for set_name in unique_set:
        pattern_dict = dict(zip(patterns[patterns['set'] == set_name]['mc'], patterns[patterns['set'] == set_name]['pattern']))
        globals()[set_name] = {key: fr"{value}" for key, value in pattern_dict.items()}

    url = url[url.URL != 'WIP']
    url = url[url.URL != '-']

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
        pdf.append('\n- '.join(pdf_files) if pdf_files else 'x')
        scrape = get_all_text(rl)
        ws.append([scrape])
        timestamp.append(current_datetime)
        time.sleep(5)
        progress_bar.progress(int((idx + 1) * progress_step))

    cleaned = []
    for web in ws:
        for text in web:
            clean = cleanText(str(text))
            cleaned.append(clean)

    url['Manual-Fact-Sale Sheet'] = pdf
    ability = []
    for row in url['Manual-Fact-Sale Sheet']:
        if row == 'x':
            ability.append('FALSE')
        else:
            ability.append('TRUE')

    url['timestamp'] = timestamp
    url['scraped'] = cleaned
    url['ability'] = ability

    url_ = url[url.ability != 'FALSE']
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
        url = row['URL']
        pdf = row['Manual-Fact-Sale Sheet']
        timestamp = row['timestamp']
        clean_ws = cleanText(scraped)

        if product not in data:
            data[product] = {
                "Group": group,
                "Abbreviation": bank_abb,
                "FI": bank_name,
                "FI_type": type_,
                "product_type": product_type,
                "URL": url,
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
                    "Sentences": '\n- '.join(matched[key]) if matched[key] else f"ไม่พบข้อความเกี่ยวกับ '{key}'"
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
    # st.write(":sparkler: Scraped Information :sparkler:")
    # st.write(scraped_data)

#### -----------filter and display------------ ####

if 'scraped_data' in st.session_state:
    scraped_data = st.session_state['scraped_data']
    group_filter = st.multiselect("Select Group", options=scraped_data["Group"].unique(), default=scraped_data["Group"].unique())
    
    filtered_group = scraped_data[scraped_data["Group"].isin(group_filter)]
    
    fi_filter = st.multiselect("Select FI", options=filtered_group["FI"].unique(), default=filtered_group["FI"].unique())
    keyword_set_filter = st.multiselect("Select Keyword Set", options=scraped_data["Keyword_Set"].unique(), default=scraped_data["Keyword_Set"].unique())
    
    filtered_data = scraped_data[
        (scraped_data["Group"].isin(group_filter)) &
        (scraped_data["FI"].isin(fi_filter)) &
        (scraped_data["Keyword_Set"].isin(keyword_set_filter))
    ]
    
    st.write(":sparkler: Filtered Information :sparkler:")
    st.write(filtered_data)
