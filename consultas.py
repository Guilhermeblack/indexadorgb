import pymysql
import nltk

#normalizaçao

def normalizaMaior(notas):
    menor = 0.00001
    maximo = max(notas.values())
    if maximo == 0:
        maximo = menor
    return dict([(id, float(nota) / maximo) for (id, nota) in notas.item()])

def normalizaMenor(notas):
    menor = 0.00001
    minimo = min(notas.values())
    return dict([(id, float(minimo) / max(menor, nota)) for (id,nota) in notas.items()])


#ordena resultados

def frequenciaScore(linhas):
    contagem = dict([linha[0], 0] for linha in linhas)
    for linha in linhas:
        # print(linha)
        contagem[linha[0]] +=1 #incrementa a frequencia da pagina
    return contagem

def localizacaoScore(linhas):  #soma os scores para classificar as mais relevantes
    localizacoes= dict([linha[0], 1000000] for linha in linhas)
    for linha in linhas:
        soma = sum(linha[1:])
        if soma < localizacoes[linha[0]]:
            localizacoes[linha[0]] = soma
    return normalizaMenor(localizacoes)

def distanciaScore(linhas):
    #linhas é o resultado da base de dados
    if len(linhas[0]) <= 2: #verificar se é mais de uma palavra pesquisada
        return dict([(linha[0], 1.0) for linha in linhas])
    distancias = dict([(linha[0], 1000000) for linha in linhas]) #define lista de tamanho padrao
    for linha in linhas:
        distancia = sum([abs(linha[x] - linha[x-1]) for x in range(2, len(linha))])
        if distancia < distancias[linha[0]]: # se a distancia entre palavras for menor que a distancia ja obtida
            distancias[linha[0]] = distancia
    return normalizaMenor(distancias) #quanto menor mais relevante a pagina

def contagemLinkScore(linhas):
    contagem = dict([linha[0], 1.0] for linha in linhas)
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    cursor= conexao.cursor()
    for x in contagem:
        cursor.execute('select count(*) from url_ligacao where idurl_destino = %s', x)
        contagem[i] = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return normalizaMaior(contagem)

def calculaPageRank(iteracoes):
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True,)
    cursorlimpa = conexao.cursor()
    cursorlimpa.execute('delete from page_rank')
    cursorlimpa.execute('insert into page_rank select idurl, 1.0 from urls') # o processo acima reicinializa a tabela
    for x in range(iteracoes):
        print('iteracao', str(x +1))
        cursorUrl= conexao.cursor()
        cursorUrl.execute('select idurl from urls')
        for url in cursorUrl:
            pgrank = 0.15 #desencorajamento %
            cursorlink = conexao.cursor()
            cursorlink.execute('select distinct(idurl_origem) from url_ligacao where idurl_destino = %s', url[0])
            for link in cursorlink:
                cursorpgrank = conexao.cursor()
                cursorpgrank.execute('select nota from page_rank where idurl = %s', link[0])
                linkpgrank = cursorpgrank.fetchone()[0]
                cursorqnt = conexao.cursor()
                cursorqnt.execute('select count(*) from url_ligacao where idurl_origem = %s', link[0])
                linkqnt = cursorqnt.fetchone()[0]
                pr += 0.85 * (linkpgrank/ linkqnt)
            cursoratualiza = conexao.cursor()
            cursoratualiza.execute('update page_rank set nota = %s where idurl = %s',(pr, url[0]))

    cursoratualiza.close
    cursorqnt.close()
    cursorpgrank.close()
    cursorlink.close()
    cursorUrl.close()
    cursorlimpa.close()
    conexao.close()

def pagRankScore(linhas):
    pgrank = dict([linha[0], 1.0] for linha in linhas)
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True,)
    cursor = conexao.cursor()
    for x in pgrank:
        cursor.execute('select nota from page_rank where idurl = %s', x)
        pgrank[x] = cursor.fetchone()[0]

    cursor.close()
    conexao.close()
    return normalizaMaior(pgrank)

def TextoLinkScore(linhas, palavrasid):
    contagem = dict([linha[0], 0] for linha in linhas)
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice', autocommit=True, )

    for id in palavrasid:
        cursor = conexao.cursor()
        cursor.execute('select ul.idurl_origem, ul.idurl_destino from url_palavra up inner join url_ligacao ul on up.idurl_ligacao = ul.idurl_ligacao where up.idpalavra = %s', id)
        for (idurl_origem, idurl_destino) in cursor:
            if idurl_destino in contagem:
                cursorrank = conexao.cursor()
                cursorrank.execute('select nota from page_rank where idurl = %s', idurl_origem)
                pr = cursorrank.fetchone()[0]
                contagem[idurl_destino] += pr

    cursorrank.close()
    cursor.close()
    conexao.close()
    return normalizaMaior(contagem)


def pesquisa(consulta):
    linhas, palavrasid = buscaVariasPalavras(consulta)
    # scores = contagemLinkScore(linhas)
    # mostra a id da url com o score de acordo com a posição
    scores = pagRankScore(linhas)
    # for url, score in score.itens():
        # print(str(url), ' - ', str(score))
    ordenaScore = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
    for (score, idurl) in ordenaScore[0:10]:
        print('%f\t%s' % (score, getUrl(idurl)))


def pesquisaPeso(consulta):
    linhas, palavrasid = buscaVariasPalavras(consulta)
    #total é o total da lista dos scores
    total = dict([linha[0], 0] for linha in linhas)
    pesos = [(1.0, frequenciaScore(linhas)),
             (1.0, localizacaoScore(linhas)),
             (1.0, distanciaScore(linhas)),
             (1.0, contagemLinkScore(linhas)),
             (1.0, pagRankScore(linhas)),
             (1.0, TextoLinkScore(linhas, palavrasid))]
    for (peso, scores) in pesos:
        for url in total:
            total[url] += peso * scores[url]
    total = normalizaMaior(total)
    ordenaScore = sorted([(score, url) for (url, score) in total.items()], reverse=1)
    for (score, idurl) in ordenaScore[0:10]:
        print('%f\t%s' % (score, getUrl(idurl)))


def getUrl(idurl):
    retorno = ''
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    cursor = conexao.cursor()
    cursor.execute('select url from urls where idurl = %s', idurl)
    if cursor.rowcount > 0:
        retorno = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return retorno



# SISTEMA DE BUSCA

def buscaVariasPalavras(consulta):
    listaCampos = 'p1.idurl'
    listaTabela = ''
    listaClausulas = ''
    palavrasid = []
    palavras = consulta.split('')
    numeroTabela = 1

    for palavra in palavras:
        idpalavra = getIdPalavra(palavra)
        if idpalavra > 0:
            palavrasid.append(idpalavra)
            if numeroTabela > 1:
                listaTabela += ', '
                listaClausulas += 'and'
                listaClausulas += 'p%d.idurl = p%d.idurl and ' % numeroTabela - 1, numeroTabela
            listaCampos += 'p%d.localizacao' % numeroTabela
            listaTabela += 'palavra_localizacao p%d' % numeroTabela
            listaClausulas += 'p%d idpalavra = %d' % (numeroTabela, idpalavra)
            numeroTabela += 1
    consultaSql= 'select %s from %s where %s' % (listaCampos, listaTabela, listaClausulas)
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    cursor = conexao.cursor()
    cursor.execute(consultaSql)
    linhas = [linha for linha in cursor]

    cursor.close()
    conexao.close()
    return linhas, palavrasid


def getIdPalavra(palavra):
    retorno = -1
    stemmer = nltk.stem.RSLPStemmer()
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    cursor = conexao.cursor()

    cursor.execute('select idpalavra from palavra where palavra = %s', stemmer.stem(palavra))
    if cursor.rowcount > 0:
        retorno = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return retorno


def buscaUmaPalavra(palavra):
    idpalavra = getIdPalavra(palavra)
    conexao = pymysql.connect(host='localhost', user='root', passwd='', db='indice')
    cursor = conexao.cursor()
    cursor.execute(
        'select urls.url from palavra_localizacao plc inner join urls on plc.idurl = urls.idurl where plc.idpalavra = %s',
        idpalavra)
    paginas = set()
    for url in cursor:
        print(url[0])
        paginas.add(url[0])
    print('paginas encontradas >> ', str(len(paginas)))
    cursor.close()
    conexao.close()
