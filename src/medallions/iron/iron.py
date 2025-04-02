""" module docstring """
from bs4 import BeautifulSoup
import requests

def get_hist_data():
    """function docstring:
    input: 
    """
    url = "https://www.google.com/finance/quote/.INX:INDEXSP?hl=en&window=MAX"
    v = requests.get(url, timeout=600)
    h = BeautifulSoup(v.text, 'html')
    return h
