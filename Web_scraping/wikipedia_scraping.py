import pandas as pd
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

def extract_wiki_info(DOM_component):
    exp = DOM_component
    link = []
    company_name = []
    industry = []
    
    
    try: 
        cols = [exp[0].find_all('th')[0].text.replace('\n',''), exp[0].find_all('th')[1].text.replace('\n','')]
        print (cols)
        for i in range(1, len(exp)):
            if exp[i].find_all('td')[0].find('a') is not None:
                if exp[i].find_all('td')[0].find('a')['href'] is not None or len(exp[i].find_all('td')[0].find('a')['href'])>0:
                    link.append(str_wiki + exp[i].find_all('td')[0].find('a')['href'])
                else:
                    link.append('')

                if exp[i].find_all('td')[0].find('a')['title'] is not None and len(exp[i].find_all('td')[0].find('a')['title'])>0:
                    company_name.append(exp[i].find_all('td')[0].find('a')['title'])
            else:
                company_name.append(exp[i].find_all('td')[0].text)
                link.append('')

            if exp[i].find_all('td')[1].text.replace('\n','') is not None and len(exp[i].find_all('td')[1].text.replace('\n',''))>0:
                industry.append(exp[1].find_all('td')[1].text.replace('\n',''))
            else:
                industry.append('')
                
        return link, company_name, industry
    
    except Exception as exception:
        print ('\t Exception in extraction :: {}'.format(url)  )

        
        

        
        
if __name__ == "__main__":
    
    url = "https://en.wikipedia.org/wiki/List_of_companies_based_in_New_York_City"
    str_wiki = 'https://en.wikipedia.org/'
    page = requests.get(url)
    if (page.status_code == 200):
        html_text = BeautifulSoup(page.text, "html.parser")
    exp = (html_text.find_all('table')[0].find_all('tr'))
    link, company_name, industry = extract_wiki_info(exp)
    df = pd.DataFrame()
    df['URL'] = link
    df['Company_Name'] = company_name
    df['Industry'] = industry
    print (df.head())
    dict_res = df.to_dict(orient="records")
    print (dict_res[0])
    
    # Connecting to NOSQL DB and saving it as collections.
    client_remote = MongoClient("mongodb://{}:{}/".format("<Ip address>", "<port number>"))
    db = client_reomote['scraping-db']
    new_collection = df['wiki']
    new_collection.insert_many(dict_res)
    
    print ('completed')
               
               
               