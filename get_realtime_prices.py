from bs4 import BeautifulSoup
import requests as r

class PriceGetter:

    def get_price(self, ticker): ## Gets the real-time price of any stock via marketwatch
        url = f'https://www.marketwatch.com/investing/stock/{ticker}'
        page = r.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        price = soup.find('bg-quote', {'class': 'value'})
        print(price.get_text())

    def get_volatility(self): ## Gets the CBOE volatility index at the time
        url = 'https://www.marketwatch.com/investing/stock/AAPL'
        page = r.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        index = soup.find('bg-quote', {'class': 'value'})
        print(index.get_text())
        return(index.get_text())
    
#priceGetter = PriceGetter()
#priceGetter.get_volatility()
