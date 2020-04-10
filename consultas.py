import pymysql
import nltk


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
