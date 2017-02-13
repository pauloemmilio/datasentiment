import oauth2
import json
import urllib.parse
import random
import sys
import time
import psycopg2

#================================   FUNCTIONS   ================================#

def select_index(index):
    if(index != -1):
        if(index == 0):
            return 1
        elif(index == 1):
            return 2
        elif(index == 2):
            return 3
        elif(index == 3):
            return 0
    else:
        return random.randint(0,3)

def delete_tweet(c, position):
    c.execute("DELETE FROM datasentiment WHERE ID = " + str(position))
            
def check_emoji(c, counter):
    c.execute("SELECT TWEET FROM datasentiment WHERE ID = " + str(counter))
    rows = c.fetchall()
    for row in rows[0]:
        for k in row:
            if(k == "�"):
                return False
    return True

def check_tweet(tweet, c):
    c.execute("SELECT * FROM datasentiment")
    rows = c.fetchall()
    for row in rows:
        if('"' in tweet['text']):
            return False
        if("'" in tweet['text']):
            return False
        if(tweet['text'][:4] == "RT @"):
            return False
        if(str(row[1]) == str(tweet['id'])):
            return False
        for k in row:
            if(k == "'" or k == '"'):
                return False
    return True
    
def get_client():        
    consumer_key = ''
    consumer_secret = ''
    token_key = ''
    token_secret = ''
    consumer = oauth2.Consumer(consumer_key, consumer_secret)
    token = oauth2.Token(token_key, token_secret)
    cliente = oauth2.Client(consumer, token)
    return cliente

def bd_counter(c):
    c.execute("SELECT ID FROM datasentiment")
    rows = c.fetchall()
    return len(rows)

#================================   END FUNCTIONS   ================================#

#   CRIAR CONEXÃO COM O BANCO
con = psycopg2.connect(host='localhost', user = 'postgres', password = '', dbname = 'datasentiment')
print("conexão realizada com o banco")

#   CRIAÇÃO DO CURSOR AUXILIAR
c = con.cursor()
print('cursor criado')

#   CRIAÇÃO DA TABELA
#c.execute('CREATE TABLE datasentiment (ID INT PRIMARY KEY NOT NULL, TWEET_ID TEXT, USERNAME TEXT NOT NULL, TWEET TEXT NOT NULL)')
#print('tabela criada')

counter = bd_counter(c)
filters = [':)',':(',':-)',':-(']
index = -1
non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

while True:
    flag = True
    index = select_index(index)
    query = filters[index]
    query_codificada = urllib.parse.quote(query, safe='')
    try:
        client = get_client()
        requisicao = client.request('https://api.twitter.com/1.1/search/tweets.json?q=' + query_codificada + '&lang=pt&result_type=mixed')
        decodificar = requisicao[1].decode()
        objeto = json.loads(decodificar)
    except Exception:
        flag = False
        print('sem conexão com a internet...')
        print('tentando reconectar...')
        time.sleep(60)
        pass
    if(flag):
        try:
            twittes = objeto['statuses']
            for twit in twittes:
                if(check_tweet(twit, c)):
                    sqlquery = "INSERT INTO datasentiment (ID, TWEET_ID, USERNAME, TWEET) VALUES (" + str(counter+1) + ", '" + str(twit['id']) + "', '" + twit['user']['screen_name'] + "', '" + twit['text'].translate(non_bmp_map)+"')"
                    c.execute(sqlquery)
                    con.commit()
                    counter+=1
                    if(check_emoji(c, counter) == False):
                        delete_tweet(c, counter)
                        con.commit()
                        print("========= REMOVEU UM TWEET =========")
                        counter-=1
                    print("inseriu " , counter , " tweet(s)")
        except Exception:
            counter = bd_counter(c)
            con.commit()
            print("até o momento foram inseridos " + str(counter) + " tweet(s) no banco de dados...")
            for i in range(15):
                print("aguarde " , 15 - i , " minutos")
                time.sleep(65)
            pass
    counter = bd_counter(c)
    con.commit()
    if(counter>=15000):
        print("finalizado")
        break
con.commit()
con.close()
