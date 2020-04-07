import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import nltk
import pymysql


def inserePalavraLocal(idurl, idpalavra, localizacao):
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute("insert into palavra_localizacao (idurl, idpalavra, idpalavra_localizacao) values (%s, %s, %s) ", (
        idurl,
        idpalavra,
        localizacao
    ))
    idpalavra_localizacao = cursor.lastrowid
    cursor.close()
    conexao.close()
    return idpalavra_localizacao


def incluirPalavra(palavra): #inclui as palavras no indice
    conexao = pymysql.connect(
        host='localhost',
        user='root',
        passwd='',
        db='indice',
        autocommit=True,
        use_unicode=True,
        charset='utf8mb4'
    )
    cursor = conexao.cursor()
    cursor.execute('insert into palavras (palavra) values (%s)', palavra)
    idpalavra = cursor.lastrowid #pega o id que acabou de ser inserido
    cursor.close()
    conexao.close()
    return idpalavra

def verificaPalavra(palavra): #verifica se a palavra exste no indice
    retorno = -1 #palavra nao existe
    conexao = pymysql.connect(
        host='localhost',
        user='root',
        passwd='',
        db='indice',
        use_unicode=True, #indica que ira trabalhar com a formataçao de texto em portugues
        charset='utf8mb4' # /\
    )
    cursor = conexao.cursor()
    cursor.execute('select idpalavra from palavras where palavra = %s', palavra)
    if cursor.rowcount > 0:
        print('palavra cadastrada')
        retorno = cursor.fetchone()[0] #retorna o id da palavra
    else:
        print('palavra nao cadastrada')
    cursor.close()
    conexao.close()
    return retorno


def inserirPag(url):
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute('insert into urls(url) values (%s)', url)
    idpagina = cursor.lastrowid  #pega o id que acabou de ser inserido
    cursor.close()
    conexao.close()
    return idpagina


def paginaIndexada(url): #verifica se a pagina ja esta na vbase de dados
    retorno = -1
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    sqlUrl = conexao.cursor()
    sqlUrl.execute('select idurl from urls where url = %s ', url)
    if sqlUrl.rowcount > 0:
        idurl = sqlUrl.fetchone()[0]
        idurl.execute('select idurl from palavras_localizacao where idurl = %s', idurl)
        if sqlUrl.rowcount > 0:
            retorno = -2
        else:
            retorno = idurl
    else:
        print('nao deu bom')

    sqlUrl.close()
    conexao.close()
    return retorno

def separaPalavra(txt):
    stops = nltk.corpus.stopwords.words('portuguese')  # pega as stop words da biblioteca nltk
    stops.append('é')
    stemmer = nltk.stem.RSLPStemmer()
    spliter = re.compile('\\w+')  # expressa oregular para formatar as palavras
    lista_palavras = []
    lista = [p for p in spliter.split(txt) if p != '']
    for x in lista:
        if x.lower() not in stops:
            if len(x) > 1:
                lista_palavras.append(stemmer.stem(x).lower())
    return lista_palavras


def getTexto(sopa): # remove as tags html do resultado
    for tag in sopa(['script', 'style']):
        tag.decompose()
    return ' '.join(sopa.stripped_strings)


def indexador(url, sopa): #verificar e indexar as paginas (funçoes acima)
    indexada = paginaIndexada(url)
    if indexada == -2:
        print('pagina indexada !')
        return
    elif indexada == -1:
        idnova_pagina = inserirPag(url)
    elif indexada > 0: #existe pagina amis n existe palavra
        idnova_pagina = indexada

    print('indexando', url)
    texto = getTexto(sopa) #texto inteiro
    palavras = separaPalavra(texto) #faz tratamento do texto retornado
    for x in range(len(palavras)):
        palavra = palavras[x]
        idpalavra = verificaPalavra(palavra)
        if idpalavra == -1:
            idpalavra = incluirPalavra(palavra)
        inserePalavraLocal(inserePalavraLocal(idnova_pagina, idpalavra, x)) #adiciona as palavras contidas no banco



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

        indexador(pg, sopa) # aqui chama o processo de tratamento e indexaçao da pagina

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