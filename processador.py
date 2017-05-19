import psycopg2
import nltk
from nltk.tokenize import word_tokenize
from sklearn import svm
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import cross_val_score
from sklearn.naive_bayes import MultinomialNB
from sklearn import linear_model

def getTweetsBD(cursor):
    tweets = []
    cursor.execute("Select * from TweetsColetados")
    linhas = cursor.fetchall()
    for linha in linhas:
        tweets.append([linha[1], linha[2]])
    return tweets

def realizarStemming(palavras):
    stemmer = nltk.stem.RSLPStemmer()
    words = []
    for palavra in palavras:
        words.append(stemmer.stem(palavra))
    return words

def removerStopwords(tweet, stopwords):
    palavras = word_tokenize(tweet)
    retorno = []
    for palavra in palavras:
        if(palavra not in stopwords):
            retorno.append(palavra)
    return retorno

def montarString(palavras):
    palavrasSelecionadas = []
    for i in range(len(palavras)):
        if(palavras[i] != "@"):
            if(palavras[i-1] != "@"):
                if(palavras[i] != "#"):
                    if(palavras[i-1] != "#"):
                        if("http" not in palavras[i]):
                            if(palavras[i].isalnum() == True):
                                if(len(palavras[i]) > 1):
                                    palavrasSelecionadas.append(palavras[i])
    space = " "
    return space.join(palavrasSelecionadas)

conexao = psycopg2.connect(host='localhost', user = 'postgres', password = 'hansolo', dbname = 'TweetsColetados')
print("conexao realizada com o banco")

#   CRIAÇÃO DO CURSOR AUXILIAR
cursor = conexao.cursor()
print('cursor criado')

emoticons = [":)", ":(", ":-)", ":-("]

tweets = getTweetsBD(cursor)
stopwords = nltk.corpus.stopwords.words('portuguese')

menorTamanho = 0
excluidos = []
tweetsProcessadosPos = []
tweetsProcessadosNeg = []

for tweet in tweets:
    palavras = removerStopwords(tweet[1], stopwords)
    palavras = realizarStemming(palavras)
    tweet_processado = montarString(palavras)

    #selecionar tweets que após as stopwords e stemming não tenham texto para remover
    if(len(tweet_processado) == menorTamanho):
        if(":)" in tweet[1] or ":-)" in tweet[1]):
            excluidos.append(tweet)
        else:
            excluidos.append(tweet)
    else:
        if(":)" in tweet[1] or ":-)" in tweet[1]):
            tweetsProcessadosPos.append(tweet_processado)
        else:
            tweetsProcessadosNeg.append(tweet_processado)

flag = False
continuar = True

#caso haja diferença na quantidade de positivos e negativos que devem ser removidos
#tentar igualar pegando os que possuirem a menor quantidade de palavras
if(len(tweetsProcessadosPos) > len(tweetsProcessadosNeg)):
    while (continuar):
        menorTamanho += 1
        if(flag == False):
            print("entrou no primeiro while | " + str(len(excluidos)) + " | " + str(len(tweetsProcessadosNeg)) + " | " + str(len(tweetsProcessadosPos)))
            flag = True
        for t in tweetsProcessadosPos:            
            if(len(t) == menorTamanho):
                excluidos.append(t)
                tweetsProcessadosPos.remove(t)
            if(len(tweetsProcessadosPos) == len(tweetsProcessadosNeg)):
                continuar = False
                break
    flag = True
else:
    while (continuar) :
        menorTamanho += 1
        if(flag == False):
            print("entrou no primeiro while | " + str(len(excluidos)) + " | " + str(len(tweetsProcessadosNeg)) + " | " + str(len(tweetsProcessadosPos)))
            flag = True
        for t in tweetsProcessadosNeg:            
            if(len(t) == menorTamanho):
                excluidos.append(t)
                tweetsProcessadosNeg.remove(t)
            if(len(tweetsProcessadosNeg) == len(tweetsProcessadosPos)):
                continuar = False
                break
    


print(len(excluidos))
print(len(tweetsProcessadosNeg))
print(len(tweetsProcessadosPos))

classifier_rbf = svm.SVC()
classifier_linear = svm.SVC(kernel='linear')
classifier_liblinear = svm.LinearSVC()
gnb = MultinomialNB()
clf = linear_model.SGDClassifier()

data = tweetsProcessadosNeg + tweetsProcessadosPos

# 0 = neg / 1 = pos
labels = [0]*len(tweetsProcessadosNeg) + [1]*len(tweetsProcessadosNeg)

vectorizer = TfidfVectorizer(min_df=5, max_df = 0.8, sublinear_tf=True, use_idf=True)

data_vectors = vectorizer.fit_transform(data)

print("realizando 1° classificação")
scores1 = cross_val_score(classifier_rbf, data_vectors, labels, cv=5)
print("realizando 2° classificação")
scores2 = cross_val_score(classifier_linear, data_vectors, labels, cv=5)
print("realizando 3° classificação")
scores3 = cross_val_score(classifier_liblinear, data_vectors, labels, cv=5)
print("realizando 4° classificação")
scores4 = cross_val_score(gnb, data_vectors, labels, cv=5)
print("realizando 5° classificação")
scores5 = cross_val_score(clf, data_vectors, labels, cv=5)

print(scores1)
print(scores2)
print(scores3)
print(scores4)
print(scores5)

#dividir restante em 80/20

#classificar os 80

#testar os 20
