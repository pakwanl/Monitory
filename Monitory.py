### version 13 Sep 2024
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

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.core.os_manager import ChromeType

from tenacity import retry, wait_exponential, stop_after_attempt, RetryError
import pathlib
import textwrap
import google.generativeai as genai

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
    
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--remote-debugging-port=9222')
    
    return webdriver.Chrome(service=Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()), options = chrome_options)

def scrap(url):
    all_text = []
    pdf_urls = []
    relevant_text = []

    driver = get_driver()
    driver.set_page_load_timeout(30)

    try:
        driver.get(url)
        # accept cookies banner if found ## note to self - to consider : probably remove? idk
        try:
            possible_selectors = [
                "//button[contains(text(), 'ยอมรับ') or contains(text(), 'Accept') or contains(text(), 'I Agree') or contains(text(), 'Allow Cookies')]",
                "//a[contains(text(), 'ยอมรับ') or contains(text(), 'Accept') or contains(text(), 'I Agree') or contains(text(), 'Allow Cookies')]",
                "//div[contains(text(), 'ยอมรับ') or contains(text(), 'Accept') or contains(text(), 'I Agree') or contains(text(), 'Allow Cookies')]",
                "//button[@id='cookie-accept']",
                "//button[@class='cookie-consent']",
                "//a[@class='accept-cookie']"]
            cookie_button_found = False
            for selector in possible_selectors:
                try:
                    cookie_button = driver.find_element(By.XPATH, selector)
                    cookie_button.click()
                    print("Cookie consent accepted.")
                    cookie_button_found = True
                    break
                except NoSuchElementException:
                    continue

            if not cookie_button_found:
                print("No cookie consent button found, Proceeding without accepting cookies...")

        except NoSuchElementException:
            print("NoSuchElementException, cookie consent button not found, Proceeding without accepting cookies...")


        final_url = driver.current_url
        selenium_cookies = driver.get_cookies()
    except TimeoutException:
        print(f"Timed out loading {url}. Skipping...")
        all_text.append(f"Timed out loading {url}. Skipping...")
        pdf_urls.append(f"Timed out loading {url}. Skipping...")
        return "Timed out session, skip to the next product..."
    finally:
        driver.quit()

    # requests to fetch content
    session = requests.Session()
    for cookie in selenium_cookies:
        cookie_name = cookie['name'].encode('utf-8').decode('latin1')
        cookie_value = cookie['value'].encode('utf-8').decode('latin1')
        session.cookies.set(cookie_name, cookie_value)

    try:
        response = session.get(final_url)
        response.raise_for_status()
        page_content = response.content.decode('utf-8', errors='replace')
        soup = BeautifulSoup(page_content, 'html.parser')

        text_elements = soup.find_all(['p', 'div', 'li', 'h1', 'h2', 'h3', 'h4', 'h5'], text=True)

        for element in text_elements:
            text = element.get_text(strip=True)
            all_text.append(text)

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href and href.lower().endswith(('.pdf', '.pdf.aspx')):
                full_pdf_url = requests.compat.urljoin(final_url, href)
                pdf_urls.append(full_pdf_url)

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while making a request: {e}")
        all_text.append(f"Error occurred while making a request: {e}")
        pdf_urls.append(f"Error occurred while making a request: {e}")
        return "Error during requests, skip to the next product..."

    return ' '.join(all_text), '\n- '.join(pdf_urls)

def is_relevant(text, pattern):
   for description, patt in pattern.items():
       if re.search(patt, text):
           return True
   return False
  
def generate_content_with_retry(model, text, pdf_urls):
  try:
      # Base prompt
      focus = "interest rate"
      base_prompt = (

       f" Please summarize the following product information, with a focus on the {focus}. Include:"
       "- The **name of the product**."
       "- **Interest rate** details, such as if it is per month or per year. If only monthly interest is provided, report as 'แจ้งเฉพาะรายเดือน'."
       "- **Installment period** if applicable."
       "- Include any **warnings or important information** that customers should be aware of, such as: 'ใช้เท่าที่จำเป็นและชำระคืนได้เต็มจำนวนตามกำหนด จะได้ไม่เสียดอกเบี้ย 16% ต่อปี'."
       "- If no interest rate information is found, mention 'ไม่มีการแจ้งข้อมูลอัตราดอกเบี้ยเป็นข้อมูลข้อความ'."

       f"Additionally, if there is a link to a **PDF fact sheet** or **sale sheet** in {pdf_urls}, mention if the sheet was found. If no sheet is found, note: 'No fact/sale sheet was found during scraping, please proceed to the website manually.' "
      )

      additional_instructions = ""
      full_prompt = f"{base_prompt} {additional_instructions}; {text}"
      response = model.generate_content(full_prompt)
      return response.text
  except Exception as e:
      raise
    
def apply_summary_relevant(focus_df, model):
    summaries = []
    for idx, row in focus_df.iterrows():
        text = row['relevant']
        pdf_urls = row['pdf']
        try:
            if len(text) > 90 :
                summary = generate_content_with_retry(model, text, pdf_urls)
                summaries.append(summary)
            else:
                summaries.append("No relevant text found.")
            time.sleep(3)  # avoid hitting API limits
        except RetryError as retry_err:
            summaries.append(f"Retries exhausted for index {idx}. Logging the issue and moving on: {retry_err}")
        except Exception as e:
            summaries.append(f"Error processing row {idx}: {e}")
    focus_df['summary_relevant'] = summaries
  
def divide_text_into_chunks(text, chunk_size=8000):
    """Divide large text into smaller chunks."""
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def apply_summary_all(focus_df, model):
    summaries = []
    for idx, row in focus_df.iterrows():
        text = row['scraped']
        pdf_urls = row['pdf']
        try:
            # Check if the text is long enough for processing
            if len(text) > 90:
                text_chunks = divide_text_into_chunks(text)
                summary_chunks = []
                for chunk in text_chunks:
                    summary_chunk = generate_content_with_retry(model, chunk, pdf_urls)
                    summary_chunks.append(summary_chunk)
                    time.sleep(3)
                full_summary = ' '.join(summary_chunks)
                summaries.append(full_summary)
            else:
                summaries.append("No text found.")
        except RetryError as retry_err:
            summaries.append(f"Retries exhausted for index {idx}. Logging the issue and moving on: {retry_err}")
        except Exception as e:
            summaries.append(f"Error processing row {idx}: {e}")
    focus_df['summary_scraped'] = summaries
  
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Output')
    workbook = writer.book
    worksheet = writer.sheets['Output']  
    writer.close()
    processed_data = output.getvalue()
    return processed_data

def update_patterns(patt):
    unique_set = patt['set'].unique()
    patterns = {}
    for set_name in unique_set:
        pattern_dict = dict(zip(patt[patt['set'] == set_name]['topic'], patt[patt['set'] == set_name]['pattern'])) 
        patterns.update({key: fr"{value}" for key, value in pattern_dict.items()})

#### ---------------data import--------------- ####
uploaded_file = st.file_uploader("Upload here :lightning_cloud:", type=["xlsx"], accept_multiple_files=False)
if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file, sheet_name="product")
        df = pd.DataFrame(df)
        st.write("Product", df)
        
        patt = pd.read_excel(uploaded_file, sheet_name="pattern")
        patt = pd.DataFrame(patterns)
        update_patterns(patt)
    except Exception as e:
        st.error(f"An error occurred while reading the file: {e}")
else:
    st.warning(":receipt: fyi, It works best with **less** than 10 url samples!")

#### --------uploaded data preparation-------- ####
if uploaded_file is not None:
  bank_filter = st.multiselect("Select Group", options=df["Group"].unique(), default=df["Group"].unique())
  filtered_bank = df[df["Group"].isin(bank_filter)]
  
#### --------------web scraping-------------- ####
def scraping(df,patterns):
  scraped = []
  pdf = []
  relevant = []
  timestamp = []
  
  progress_bar = st.progress(0)
  total_urls = len(df)
  progress_step = 100 / total_urls if total_urls > 0 else 0
  idx = 0
  for url in filtered_bank['URL']:
    result = scrap(url)
    current_datetime = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(result, tuple) and len(result) == 2:
      scrap_text, _pdf, = result
      scraped.append(scrap_text)
      pdf.append(_pdf)
      _relevant = is_relevant(scrap_text,patterns) 
      relevant.append(_relevant)
      timestamp.append(current_datetime)
    else:
      scraped.append(f"Unexpected result from scrap function: {result}")
      pdf.append(f"Unexpected result from scrap function: {result}")
      relevant.append(f"Unexpected result from scrap function: {result}")
      timestamp.append(current_datetime)
    time.sleep(random.uniform(1,5))
    progress_bar.progress(int((idx + 1) * progress_step))
    
  focus_df = filtered_bank.copy()
  focus_df['timestamp'] = timestamp
  focus_df['scraped'] = scraped
  focus_df['scraped'] = focus_df['scraped'].apply(cleanText)
  focus_df['relevant'] = relevant
  focus_df['relevant'] = focus_df['relevant'].apply(cleanText)
  focus_df['pdf'] = pdf
    
  return focus_df

if uploaded_file is not None and st.button("Start Scraping!"):
    scraped_data = scraping(filtered_bank,patterns)
    st.session_state['scraped_data'] = scraped_data

#### -----------filter and display------------ ####

if 'scraped_data' in st.session_state:
    
    scraped_data = st.session_state['scraped_data']
    group_filter = st.multiselect("Select Group", options=scraped_data["Group"].unique(), default=scraped_data["Group"].unique())
    filtered_group = scraped_data[scraped_data["Group"].isin(group_filter)]
    
    fi_filter = st.multiselect("Select FI", options=filtered_group["FI_name"].unique(), default=filtered_group["FI_name"].unique())
    filtered_fi = filtered_group[filtered_group["FI_name"].isin(fi_filter)]
    
    product_type_filter = st.multiselect("Select product type", options=filtered_fi["Product_type"].unique(), default=filtered_fi["Product_type"].unique())
    filtered_type_product = filtered_fi[filtered_fi["Product_type"].isin(product_type_filter)]
    
    filtered_data = scraped_data[
        (scraped_data["Group"].isin(group_filter)) &
        (scraped_data["FI_name"].isin(fi_filter)) &
        (scraped_data["Product_type"].isin(product_type_filter))]
    
    current_datetime = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%d")
    st.write(":sparkler: Filtered Information :sparkler:")
    st.write(filtered_data)
    df_xlsx = pd.DataFrame(scraped_data)
    xlsx = to_excel(df_xlsx)
    st.download_button(label='📥 Download Scraped File',
                                    data=xlsx ,
                                    file_name= f"output_{current_datetime}.xlsx")
    if st.button("Summary") :
        api_key = st.secrets["API"]
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        apply_summary_relevant(focus_df, model)
        apply_summary_all(focus_df, model)
        xlsx = to_excel(focus_df)
        st.download_button(label='📥 Download summarized File',
                                        data=xlsx ,
                                        file_name= f"output_{current_datetime}.xlsx")

    

