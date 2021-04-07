from datetime import datetime
import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
import time
import pika
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish


class analitica():
    ventana = 10
    pronostico = 3
    file_name = "data_base.csv"
    servidor = "rabbit"

    def __init__(self) -> None:
        self.load_data()

    def load_data(self):

        if not os.path.isfile(self.file_name):
            self.df = pd.DataFrame(columns=["fecha", "sensor", "valor"])
        else:
            self.df = pd.read_csv (self.file_name)

    def update_data(self, msj):
        msj_vetor = msj.split(",")
        now = datetime.now()
        date_time = now.strftime('%d.%m.%Y %H:%M:%S')
        new_data = {"fecha": date_time, "sensor": msj_vetor[0], "valor": float(msj_vetor[1])}
        self.df = self.df.append(new_data, ignore_index=True)
        new_data = {"fecha": date_time, "sensor": msj_vetor[2], "valor": float(msj_vetor[3])}
        self.df = self.df.append(new_data, ignore_index=True)

        self.publicar("volumen1",msj_vetor[1])
        self.publicar("volumen2",msj_vetor[3])

        self.analitica_descriptiva()
        self.analitica_predictiva()
        self.guardar()

    def print_data(self):
        print(self.df)

    def analitica_descriptiva(self):
        self.operaciones("volumen1")
        self.operaciones("volumen2")


    def operaciones(self, sensor):
        df_filtrado = self.df[self.df["sensor"] == sensor]
        df_filtrado = df_filtrado["valor"]
        df_filtrado = df_filtrado.tail(self.ventana)
        self.publicar("max-{}".format(sensor), str(df_filtrado.max(skipna = True)))
        self.publicar("min-{}".format(sensor), str(df_filtrado.min(skipna = True)))
        self.publicar("mean-{}".format(sensor), str(df_filtrado.mean(skipna = True)))
        self.publicar("median-{}".format(sensor), str(df_filtrado.median(skipna = True)))
        self.publicar("std-{}".format(sensor), str(df_filtrado.std(skipna = True)))

        if ("max-{}".format(sensor)=="max-volumen1".format(sensor)) and str(df_filtrado.max(skipna = True))>"70":
            #publish.single('2/alarmav1', "Alberca llena", hostname='52.54.240.23', client_id='alertamicro')
            self.publicar("nivelaltov1".format(sensor), "Alberca llena")

        if ("min-{}".format(sensor)=="min-volumen1".format(sensor)) and str(df_filtrado.min(skipna = True))<"60":
            #publish.single('2/alarmav1', "Alberca vacia", hostname='52.54.240.23', client_id='alertamicro')
            self.publicar("nivelbajov1".format(sensor), "Alberca vacia")

        if ("max-{}".format(sensor)=="max-volumen2".format(sensor)) and str(df_filtrado.max(skipna = True))>"30":
            #publish.single('2/alarmav2', "Tanque elevado lleno", hostname='52.54.240.23', client_id='alertamicro')
            self.publicar("nivelaltov2".format(sensor), "Tanque elevado lleno")

        if ("min-{}".format(sensor)=="min-volumen2".format(sensor)) and str(df_filtrado.min(skipna = True))<"23":
            #publish.single('2/alarmav2', "Tanque elevado->nivel muy bajo", hostname='52.54.240.23', client_id='alertamicro')
            self.publicar("nivelaltov2".format(sensor), "Tanque elevado->nivel muy bajo")


    def analitica_predictiva(self):
        self.regresion("volumen1")
        self.regresion("volumen2")


    def regresion(self, sensor):
        df_filtrado = self.df[self.df["sensor"] == sensor]
        df_filtrado = df_filtrado.tail(self.ventana)
        df_filtrado['fecha'] = pd.to_datetime(df_filtrado.pop('fecha'), format='%d.%m.%Y %H:%M:%S')
        df_filtrado['segundos'] = [time.mktime(t.timetuple()) - 18000 for t in df_filtrado['fecha']]
        tiempo = df_filtrado['segundos'].std(skipna = True)
        if np.isnan(tiempo):
            return
        tiempo = int(round(tiempo))
        ultimo_tiempo = df_filtrado['segundos'].iloc[-1]
        ultimo_tiempo = ultimo_tiempo.astype(int)
        range(ultimo_tiempo + tiempo,(self.pronostico + 1) * tiempo + ultimo_tiempo, tiempo)
        nuevos_tiempos = np.array(range(ultimo_tiempo + tiempo,(self.pronostico + 1) * tiempo + ultimo_tiempo, tiempo))

        X = df_filtrado["segundos"].to_numpy().reshape(-1, 1)
        Y = df_filtrado["valor"].to_numpy().reshape(-1, 1)
        linear_regressor = LinearRegression()
        linear_regressor.fit(X, Y)
        Y_pred = linear_regressor.predict(nuevos_tiempos.reshape(-1, 1))
        for tiempo, prediccion in zip(nuevos_tiempos, Y_pred):
            time_format = datetime.utcfromtimestamp(tiempo)
            date_time = time_format.strftime('%d.%m.%Y %H:%M:%S')
            self.publicar("prediccion-{}".format(sensor), "{},{}".format(date_time,prediccion[0]))
            self.publicar("datopredicgrafica-{}".format(sensor), "{:.2f}".format(prediccion[0]))
    @staticmethod
    def publicar(cola, mensaje):
        connexion = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit'))
        canal = connexion.channel()
        # Declarar la cola
        canal.queue_declare(queue=cola, durable=True)
        # Publicar el mensaje
        canal.basic_publish(exchange='', routing_key=cola, body=mensaje)
        # Cerrar conexión
        connexion.close()

    def guardar(self):
        self.df.to_csv(self.file_name, encoding='utf-8')