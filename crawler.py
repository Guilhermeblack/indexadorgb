
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def crawl(pag, profundidade):
    contador = 0
    urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
    for x in profundidade:
        new_pages = set() # metodo set nao recebe dados repetidos
        for pg in pag: #percorre as paginas
            http = urllib3.PoolManager()
            try:
                dados = http.request('GET', pg)
            except:
                print('erro na page: '+ pg)
                continue

        print(dados)
        sopa = BeautifulSoup(dados.data, "lxml")
        links = sopa.find_all("a")

        for link in links: #percorre os links das paginas

            #pepar os links encontrados na query
            if ('href' in link.attrs):
                # completa os endereços que nao tem 'href' afim de avitar erros na busca
                url = urljoin(pg, str(link.get('href')))
                # se diferente de -1 é por que encontrou a url
                if url.find("'") != -1:
                    continue

                # quebra os links internos do site
                url = url.split('#')[0]
                if url[0:4] == 'http':
                    new_pages.add(url)
                contador += 1
        pag = new_pages # variavel que ira receber as paginas encontradas na url
                        #refaz todo o processo com as paginas encontradas

#pagina de busca inicial
list_pages = ['https://www.bing.com/search?q=gb&form=QBLH&sp=-1&pq=&sc=0-0&qs=n&sk=&cvid=AE9237E23D454822A85696D6335F9174']

#teste/chamada de funçao
crawl(list_pages, 2)