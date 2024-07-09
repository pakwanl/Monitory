#### ------------libraries import------------ ####

import streamlit as st
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import time
from urllib.parse import urljoin
import datetime
import pytz
import json
from streamlit_option_menu import option_menu

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
tableau_token_name = st.secrets["tableau"]["token_name"]
tableau_token_value = st.secrets["tableau"]["token_value"]
tableau_server_url = st.secrets["tableau"]["server_url"]
site_id = st.secrets["tableau"]["site_id"]

def tableau_auth():
    url = f"{tableau_server_url}/api/3.8/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": tableau_token_name,
            "personalAccessTokenSecret": tableau_token_value,
            "site": {
                "contentUrl": site_id
            }
        }
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Tableau authentication failed: {response.content}")
    return response.json()['credentials']['token']

# Publish data function
def publish_data_to_tableau(session_token, dataframe, datasource_name):
    url = f"{tableau_server_url}/api/3.8/sites/{site_id}/datasources"
    headers = {
        "X-Tableau-Auth": session_token
    }

    # Save dataframe to CSV
    dataframe.to_csv("data.csv", index=False)

    # Prepare the multipart request
    payload = {
        "datasource": {
            "name": datasource_name,
            "project": {
                "id": site_id
            }
        }
    }
    files = {
        'request_payload': (None, json.dumps(payload), 'application/json'),
        'tableau_datasource': ('data.csv', open('data.csv', 'rb'), 'application/octet-stream')
    }
    
    response = requests.post(url, headers=headers, files=files)
    if response.status_code != 201:
        raise Exception(f"Failed to publish data to Tableau: {response.content}")
    return response.json()

# Sign out function
def tableau_signout(session_token):
    url = f"{tableau_server_url}/api/3.8/auth/signout"
    headers = {
        "X-Tableau-Auth": session_token
    }
    requests.post(url, headers=headers)

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

uploaded_file = st.file_uploader("Upload here :lightning_cloud:", type=None, accept_multiple_files=False)
if uploaded_file is not None:
    url = pd.read_excel(uploaded_file, sheet_name="product")
    url = pd.DataFrame(url)

    patterns = pd.read_excel(uploaded_file, sheet_name="pattern")
    patterns = pd.DataFrame(patterns)

    st.write("Product", url)
    st.write("Keyword pattern", patterns)
else:
    st.warning(":receipt: waiting for uploaded file to continue!")

#### --------uploaded data preparation-------- ####

if uploaded_file is not None:
    unique_set = patterns['set'].unique()
    for set_name in unique_set:
        pattern_dict = dict(zip(patterns[patterns['set'] == set_name]['mc'], patterns[patterns['set'] == set_name]['pattern']))
        globals()[set_name] = {key: fr"{value}" for key, value in pattern_dict.items()}

    url = url[url.URL != 'WIP']
    url = url[url.URL != '-']
    url = url[url.Note == 'keep']

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
                "timestamp": details["timestamp"],
                "Keyword_Set": keyword_info["keyword_set"],
                "keyword": keyword_info["keyword"],
                "Sentences_found": keyword_info["Sentences_found"],
                "Sentences": keyword_info["Sentences"]
            })

    return pd.DataFrame(flattened)

if uploaded_file is not None:
    st.button("Start Scraping!"):
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
    
    st.write(":sparkler: Filtered Information :sparkler:")
    st.write(filtered_data)
    
    if st.button("Publish to Tableau"):
            try:
                session_token = tableau_auth()
                publish_data_to_tableau(session_token, filtered_data, "Scraped Data")
                tableau_signout(session_token)
                st.success("Data published to Tableau successfully!")
            except Exception as e:
                st.error(f"Failed to publish data to Tableau: {e}")
