from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import nltk
import pymysql
import urllib3


def inserePalavraLocal(idurl, idpalavra, localizacao):
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute("insert into palavra_localizacao (idurl, idpalavra, idpalavra_localizacao) values (%s, %s, %s) ", (idurl, idpalavra, localizacao))
    idpalavra_localizacao = cursor.lastrowid
    print('entidade de relacionamento > ', idpalavra_localizacao)
    cursor.close()
    conexao.close()
    return idpalavra_localizacao

def inserirLigacaoUrl(idurlOrigem, idurlDestino):
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute('insert into url_ligacao(idurl_origem, idurl_destino) values (%s, %s)', (idurlOrigem, idurlDestino))
    idurl_ligacao = cursor.lastrowid
    cursor.close()
    conexao.close()
    return idurl_ligacao

def inserirUrlPalavra(idpalavra, idurl_ligacao):
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute('insert into url_palavra(idpalavra, idurl_ligacao) values (%s, %s)',(idpalavra, idurl_ligacao))
    idurl_palavra = cursor.lastrowid
    cursor.close()
    conexao.close()
    return idurl_palavra

    #verificar se existe ligacao entra as duas palavras
def getIdurlLigacao(idurl_origem, idurl_destino):
    idurl_ligacao = -1
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    cursor = conexao.cursor()
    cursor.execute('select idurl_ligacao from url)ligacao where idurl_origem = %s and idurl_destino = %s', (idurl_origem, idurl_destino))
    if cursor.rowcount > 0: # verifica se retorna o id de registro do bd
        idurl_ligacao = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return idurl_ligacao

def getIdUrl(url): #pega a respectiva url no bd
    idurl = -1
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
    cursor.execute('select idurl from urls where url = %s', url)
    if cursor.rowcount > 0:
        idurl = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return idurl


def incluirPalavra(palavra):  # inclui as palavras no indice
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
    idpalavra = cursor.lastrowid  # pega o id que acabou de ser inserido
    print('id da palavra inserido ', idpalavra)
    cursor.close()
    conexao.close()
    return idpalavra


def verificaPalavra(palavra):  # verifica se a palavra exste no indice
    retorno = -1  # palavra nao existe
    conexao = pymysql.connect(
        host='localhost',
        user='root',
        passwd='',
        db='indice',
        use_unicode=True,  # indica que ira trabalhar com a formataçao de texto em portugues
        charset='utf8mb4'  # /\
    )
    cursor = conexao.cursor()
    cursor.execute('select idpalavra from palavras where palavra = %s', palavra)
    if cursor.rowcount > 0:
        retorno = cursor.fetchone()[0]  # retorna o id da palavra
        print('palavra cadastrada', retorno)
    else:
        print('palavra nao cadastrada')
    cursor.close()
    conexao.close()
    return retorno


def inserirPag(url):
    conexao = pymysql.connect(
        host='localhost',
        user='root',
        passwd='',
        db='indice',
        autocommit=True,
        use_unicode=True,
        charset='utf8mb4')
    cursor = conexao.cursor()
    cursor.execute('insert into urls(url) values (%s)', url)
    idpagina = cursor.lastrowid  # pega o id que acabou de ser inserido
    print('id da pagina acabou de ser inserido', idpagina)
    cursor.close()
    conexao.close()
    return idpagina


def paginaIndexada(url):  # verifica se a pagina ja esta na vbase de dados
    retorno = -1
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    sqlUrl = conexao.cursor()
    sqlUrl.execute('select idurl from urls where url = %s ', url)
    if sqlUrl.rowcount > 0:
        idurl = sqlUrl.fetchone()[0]
        sqlUrl.execute('select idurl from palavra_localizacao where idurl = %s', idurl)
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
    txt = txt.replace('_', ' ')
    stops = nltk.corpus.stopwords.words('portuguese')  # pega as stop words da biblioteca nltk
    # print(stops)
    stemmer = nltk.stem.RSLPStemmer()
    spliter = re.compile('\\W+')  # expressa oregular para formatar as palavras
    lista_palavras = []
    lista = [p for p in spliter.split(txt) if p != '']
    # print('lista madldita >>', lista)
    for x in lista:
        if x.lower() not in stops:
            if len(x) > 1:
                lista_palavras.append(stemmer.stem(x).lower())
    # print('lista de palaras \n', lista_palavras)
    return lista_palavras


def urlLigaPalavra(url_origem, url_destino):
    texto_url = url_destino.replace('_',' ')
    palavraas = separaPalavra(texto_url)
    idurl_origem = getIdUrl(url_origem)
    idurl_destino = getIdUrl(url_destino)
    if idurl_destino == -1:
        idurl_destino = inserirPag(url_destino)
    if idurl_origem == idurl_destino:
        return
    if getIdurlLigacao(idurl_origem, idurl_destino)> 0:
        return
    idurl_ligacao = inserirLigacaoUrl(idurl_origem, idurl_destino)
    for palavra in palavras:
        idpalavra = verificaPalavra(palavra)
        if idpalavra == -1:
            idpalavra = incluirPalavra(palavra)
        inserirUrlPalavra(idpalavra, idurl_ligacao)


def getTexto(sopa):  # remove as tags html do resultado
    for tag in sopa(['script', 'style']):
        tag.decompose()
    return ' '.join(sopa.stripped_strings)


def indexador(url, sopa):  # verificar e indexar as paginas (funçoes acima)
    indexada = paginaIndexada(url)
    if indexada == -2:
        print('pagina indexada !')
        return
    elif indexada == -1:
        idnova_pagina = inserirPag(url)
        print('pagina nao indexada, indexando agora... \n')
    elif indexada > 0:  # existe pagina mas n existe palavra indexada
        idnova_pagina = indexada
        print('id da pagina')

    texto = getTexto(sopa)  # texto inteiro
    palavras = separaPalavra(texto)  # faz tratamento do texto retornado
    for x in range(len(palavras)):
        palavra = palavras[x]
        idpalavra = verificaPalavra(palavra)
        if idpalavra == -1:
            idpalavra = incluirPalavra(palavra)
        inserePalavraLocal(idnova_pagina, idpalavra, x)  # adiciona as palavras contidas no banco


def crawl(pag, profundidade):
    contador = 0
    x=0
    urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)
    while x < profundidade:
        new_pages = set()  # metodo set nao recebe dados repetidos
        for pg in pag:  # percorre as paginas
            print('pg da variavel pag que sera tratada ', pg)
            http = urllib3.PoolManager()
            try:
                dados = http.request('GET', pg)
            except:
                print('erro na page: ' + pg)
                continue

        print('requisição', dados)
        sopa = BeautifulSoup(dados.data, "lxml")

        indexador(pg, sopa)  # aqui chama o processo de tratamento e indexaçao da pagina

        links = sopa.find_all("a")

        for link in links:  # percorre os links das paginas
            # print('variavel link em uso... ', link)
            # pepar os links encontrados na query
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
                urlLigaPalavra(pg, url)
                contador += 1
        # print('new pages que ira servir a pagina ', new_pages)
        pag = new_pages  # variavel que ira receber as paginas encontradas na url
        # print('variavel pagina de link encontrado na pagina ',pag)
        # refaz todo o processo com as paginas encontradas
        x+=1

# pagina de busca inicial
list_pages = ['https://www.uninter.com/noticias/lei-geral-de-protecao-de-dados-o-que-eu-pessoa-fisica-ou-microempresa-tenho-a-ver-com-ela']

# teste/chamada de funçao
crawl(list_pages, 2)
