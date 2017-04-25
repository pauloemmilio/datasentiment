import oauth2
import json
import urllib.parse
import random
import sys
import time
import psycopg2
from datetime import datetime, timedelta

#================================   FUNÇÕES   ================================#

#VERIFICAR A POLARIDADE DE UM TWEET
def verificarPolaridade(tweet):
    emoticons = [":)", ":(", ":-(", ":-)"]
    emoticonsPositivos = [":)", ":-)"]
    emoticonsNegativos = [":(", ":-("]
    contadorDePositivos = 0
    contadorDeNegativos = 0
    for caractere in emoticons:
        if(caractere in tweet) and (caractere in emoticonsPositivos):
            contadorDePositivos += 1
        elif(caractere in tweet) and (caractere in emoticonsNegativos):
            contadorDeNegativos += 1
    if(contadorDeNegativos > 0) and (contadorDePositivos == 0):
        return "negativo"
    elif(contadorDePositivos > 0) and (contadorDeNegativos == 0):
        return "positivo"
    else:
        return "neutro"

#VERIFICAR SE UM TWEET COM DETERMINADA POLARIDADE AINDA PODE FAZER PARTE DA SUA RESPECTIVA LISTA
def validarLista(polaridade, listaPositiva, listaNegativa, limiteDeTweets):
    if(polaridade == "positivo"):
        if(len(listaPositiva) >= 0) and (len(listaPositiva) < limiteDeTweets):
            return True
        else:
            return False
    elif(polaridade == "negativo"):
        if(len(listaNegativa) >= 0) and (len(listaNegativa) < limiteDeTweets):
            return True
        else:
            return False
    elif(polaridade == "neutro"):
        return False

#VERIFICAR SE JÁ NÃO EXISTE UM TWEET COM O MESMO ID EM ALGUMA DAS LISTAS
def validarId(id, listaPositiva, listaNegativa, listaGeral):
    for tweet in listaGeral:
        if(id == tweet[0]):
            return False
    for tweet in listaPositiva:
        if(id == tweet[0]):
            return False
    for tweet in listaNegativa:
        if(id == tweet[0]):
            return False
    return True

#VERIFICAR SE O TWEET NÃO POSSUI EMOJIS QUE NÃO SÃO CAPAZES DE SEREM LIDOS
def validarEmoji(tweet):
    for caractere in tweet:
        if(caractere == "�"):
            return False
    return True

#VERIFICAR SE NÃO EXISTE ALGUM TWEET COM O MESMO TEXTO NAS LISTAS
def validarTextoNaLista(texto, listaNegativa, listaPositiva, listaGeral):
    for tweet in listaGeral:
        if(texto == tweet[1]):
            return False
    for tweet in listaPositiva:
        if(texto == tweet[1]):
            return False
    for tweet in listaNegativa:
        if(texto == tweet[1]):
            return False
    return True

#VALIDAR O TEXTO PARA EVITAR TWEETS COM ASPAS, RETWEETS E TWEETS QUE NÃO CONTENHAM TEXTO
def validarTexto(tweet):
    if("'" in tweet):
        return False
    elif("((" in tweet) or ("))" in tweet):
        return False
    elif(tweet[:4] == "RT @"):
        return False
    elif(verificarSeContemTexto(tweet) == False):
        return False
    else:
        return True

#VERIFICAR SE CONTÉM TEXTO ALÉM DE EMOTICONS, LINKS E USERNAMES
def verificarSeContemTexto(tweet):
    emoticons = [":)", ":(", ":-(", ":-)"]
    palavras = tweet.split(" ")
    for palavra in palavras:
        if(palavra not in emoticons) and (palavra[0] != "@") and ("http" not in palavra):
            return True
    return False

#REALIZAR A VALIDAÇÃO GERAL
def validarTweet(id, tweet, listaPositiva, listaNegativa, listaGeral, limiteDeTweets):
    if(validarId(id, listaPositiva, listaNegativa, listaGeral)):
        if(validarEmoji(tweet)):
            if(validarTextoNaLista(tweet, listaNegativa, listaPositiva, listaGeral)):
                if(validarTexto(tweet)):
                    return True
    return False

#PEGAR UM CLIENTE DO TWITTER PARA REALIZAR A CAPTURA
def get_cliente(ck, cs, tk, ts, indice):
    consumer = oauth2.Consumer(ck[indice], cs[indice])
    token = oauth2.Token(tk[indice], ts[indice])
    cliente = oauth2.Client(consumer, token)
    return cliente

#CONTADOR DE LINHAS NO BANCO DE DADOS
def bd_contador(cursor):
    cursor.execute("SELECT ID FROM Teste2")
    linhas = cursor.fetchall()
    return len(linhas)

#REALIZAR O CADASTRO DOS TWEETS NO BANCO DE DADOS
def salvarTweets(listaPositiva, listaNegativa, conexao, cursor):
    for tweet in listaPositiva:
        sqlquery = "INSERT INTO Teste2 (ID, TWEET_ID, TWEET) VALUES (" + str(bd_contador(cursor)+1) + ", '" + tweet[0] + "', '" + tweet[1] +"')"
        cursor.execute(sqlquery)
        print("salvou o tweet de n° " , bd_contador(cursor))
        conexao.commit()
    for tweet in listaNegativa:
        sqlquery = "INSERT INTO Teste2 (ID, TWEET_ID, TWEET) VALUES (" + str(bd_contador(cursor)+1) + ", '" + tweet[0] + "', '" + tweet[1] +"')"
        cursor.execute(sqlquery)
        print("salvou o tweet de n° " , bd_contador(cursor))
        conexao.commit()

#================================   FIM DAS FUNÇÕES   ================================#

consumer_keys = ['XvboRjR2hR46xOdtszALnO6x9',
                 's1KSP6Vm6i3Y8LcSxiOm2NNRG',
                 '5om5GoAhQPrscLFUHoHduFrIq',
                 'uygazRzTBUWnLKreqreTTZUU4']

consumer_secrets = ['XPrDtZqa26LGYbHKy3oHFx9S28KnGCgf7t1YYCLg0ffCTB008Q',
                    '24dWKJXmPgT6Ylwcu9DQ0QCvHUS9m66w7M2cuVrluC9cTZopSR',
                    'mitUyswANwwu2frthDvsbpKkFZIbfQhxZEcRI5ns0VYhT3U5Lq',
                    'S7wDD3xD3m29LC9krtDwBQLOMjdg74LwOiufzynVB0rCwp9J5V']

token_keys = ['2488355484-rh8Nl9DfkZa1OQKSmjf04MKs4u9wDDhy96YpKHh',
              '2488355484-NdyEb5c6FXS8HmRr0OWGeRCcmYxEdocTnqj7e9V',
              '2488355484-s9i5qFgYJwZvxmTcQasg1MvHoJdUgmwCbSJ7OuX',
              '2488355484-xxYFzpgF3Wv6HThYYkdYd7uo5uGL8O7Z3bv6P9v']

token_secrets = ['bMTr9xWpnrFBXEj96YPCJHo6GCw6JSlHzjR3G1nt1TZcj',
                 'Z8p0Newb5QvhLhijApZC7MitQcOW8gyCD9XXVoYFoDNHM',
                 'uKwQcTE81FZ1aER9oGqsfjzivhK2ZJhVVe3koAQqFgx5s',
                 'ffPBvMtX1NqWM4bmuhKTzy1Svih1tZeehVf5pUpmUeiYE']

#   CRIAR conexao COM O BANCO
conexao = psycopg2.connect(host='localhost', user = 'postgres', password = 'hansolo', dbname = 'Teste2')
print("conexao realizada com o banco")

#   CRIAÇÃO DO CURSOR AUXILIAR
cursor = conexao.cursor()
print('cursor criado')

#   CRIAÇÃO DA TABELA
#cursor.execute('CREATE TABLE Teste2 (ID INT PRIMARY KEY NOT NULL, TWEET_ID TEXT, TWEET TEXT NOT NULL)')
print('tabela criada')

indice = 0
non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
query = ":) OR :( OR :-( OR :-)"
query_codificada = urllib.parse.quote(query, safe='')

listaGeral = []
limiteDeTweets = 500

inicioColeta = datetime.now()
print("inicio da coleta: ", inicioColeta)

finalColeta = inicioColeta + timedelta(hours = 6)
print("final da coleta: ", finalColeta)

inicioCiclo = inicioColeta
print("inicio do ciclo: ", inicioCiclo)

while(finalColeta > datetime.now()):
    alerta = True
    finalCiclo = inicioCiclo + timedelta(minutes = 55)
    print("final do ciclo: ", finalCiclo)

    #esperando para começar...
    while(inicioCiclo > datetime.now()):
        if(alerta):
            print("aguardando novo ciclo...")
            alerta = False

    listaPositiva = []
    listaNegativa = []

    #capturar
    while(finalCiclo > datetime.now()) and ((len(listaPositiva) < limiteDeTweets) or (len(listaNegativa) < limiteDeTweets)):
        flag = True
        try:
            cliente = get_cliente(consumer_keys, consumer_secrets, token_keys, token_secrets, indice)
            requisicao = cliente.request('https://api.twitter.com/1.1/search/tweets.json?q=' + query_codificada + '&lang=pt&result_type=mixed')
            decodificar = requisicao[1].decode()
            objeto = json.loads(decodificar)
        except Exception:
            flag = False
            print('Sem conexão com a internet... Tentando reconectar...')
            time.sleep(60)
            pass
        if(flag):
            try:
                tweet = objeto['statuses']
                for t in tweet:
                    print(t['text'].translate(non_bmp_map))
                    #avaliar a polaridade do tweet
                    polaridade = verificarPolaridade(t['text'].translate(non_bmp_map))
                    #se a lista referente a polaridade ainda couber tweets ou não for neutro
                    if(validarLista(polaridade, listaPositiva, listaNegativa, limiteDeTweets)):
                        print("passou da primeira validação")
                        #se a validação do tweet for ok
                        if(validarTweet(str(t['id']), t['text'].translate(non_bmp_map), listaPositiva, listaNegativa, listaGeral, limiteDeTweets)):
                            print("passou da segunda validação")
                            #salvar tweet na sua respectiva lista
                            if(polaridade == 'positivo'):
                                listaPositiva.append([str(t['id']), t['text'].translate(non_bmp_map)])
                                print("salvou na lista")
                            elif (polaridade == 'negativo'):
                                listaNegativa.append([str(t['id']), t['text'].translate(non_bmp_map)])
                                print("salvou na lista")
                    print("quantidade de positivos: ", len(listaPositiva))
                    print("quantidade de negativos: ", len(listaNegativa))
            except Exception:
                print("Tentando encontrar um cliente disponível... Conectando ao cliente N°: ", indice)
                if(indice == len(consumer_keys)-1):
                    indice = 0
                else:
                    indice+=1
                pass
    print("igualar listas")
    if(len(listaPositiva) != len(listaNegativa)):
        if(len(listaPositiva) > len(listaNegativa)):
            listaPositiva = listaPositiva[:len(listaNegativa)]
        else:
            listaNegativa = listaNegativa[:len(listaPositiva)]
    print(len(listaNegativa))
    print(len(listaPositiva))
    print("adicionar na lista geral")
    for tweet in listaPositiva:
        listaGeral.append(tweet)
    for tweet in listaNegativa:
        listaGeral.append(tweet)
    print("quantidade geral: ", len(listaGeral))
    #Salvar
    print("começar a salvar no banco")
    salvarTweets(listaPositiva, listaNegativa, conexao, cursor)
    inicioCiclo = finalCiclo + timedelta(minutes=5)
    if(inicioCiclo >= finalColeta):
        break
    else:
        print("inicio do ciclo: ", inicioCiclo)

print("finalizado")
print(str(bd_contador(cursor)), " tweets capturados")
conexao.commit()
conexao.close()
print("realizar validação final")
for tweet in listaGeral:
    for t in listaGeral:
        if(tweet[1] == t[1]) and (tweet[0] != t[0]):
            print(tweet)
            print(t)
            print("Achou")
