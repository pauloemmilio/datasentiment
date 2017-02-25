import psycopg2
import time
apagou = 0
iguais = 0

saved = []

#CRIANDO A CONEXÃO
con = psycopg2.connect(host='localhost', user = 'postgres', password = '', dbname = 'datasentiment')

#CRIAÇÃO DO CURSOR AUXILIAR
c = con.cursor()

c.execute("SELECT * FROM datasentiment")
con.commit()
rows = c.fetchall()

print("começou a verificar")
for k in range(len(rows)):
    print("verificando id: " + str(rows[k][0]))
    for j in range(len(rows)):
        if(rows[k][3] == rows[j][3] and rows[k][0] != rows[j][0]):
            print("=========================REPETIDOS=========================")
            print("id: ", rows[j][0])
            print("tweet id: ", rows[j][1])
            print( "texto: " , rows[j][3])
            print()
            iguais+=1
print("parou de verificar")
for k in range(len(rows)):
    for j in range(len(rows)):
        if(rows[k][3] == rows[j][3] and rows[k][0] != rows[j][0]):
            if(rows[k][3] in saved):
                c.execute("DELETE from datasentiment where ID=" + str(rows[k][0]) + "")
                print("apagou id: " , str(rows[k][0]))
                apagou+=1
            else:
                saved.append(rows[k][3])
                print("salvou id: " , str(rows[k][0]))
con.commit()

print(iguais)
print(apagou)
