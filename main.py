from starlette.middleware.cors import CORSMiddleware
import pymysql
from fastapi import FastAPI

#CONEXION

myDB = pymysql.connect(
        host="freedb.tech", 
        user="freedbtech_Noe",
        passwd="20198016", 
        port=3306,
        database="freedbtech_Vacunacion")

app=FastAPI()
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

#METODO QUE EJECUTA EL INSERT DE VACUNAS

def InsertVacuna(nombre,cantidad):
    try:
        sql = """INSERT INTO VACUNA(NOMBRE_VACUNA,CANTIDAD) VALUES(%s,%s)"""
        campos = (nombre,cantidad)
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql,campos)
        connection.commit()
        return 1
    except:
        return 0

#METODO QUE EJECUTA EL INSERT DE PROVINCIA

def InsertProvincia(nombre):
    try:
        sql = """INSERT INTO PROVINCIA(NOMBRE) VALUES(%s)"""
        campos = (nombre)
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql,campos)
        connection.commit()
        return 1
    except:
        return 0

#METODO QUE EJECUTA EL INSERT DE VACUNADOS

def InsertVacunado(cedula,nombre,apellido,telefono,vacuna,provincia):
    try:
        sql = """INSERT INTO VACUNADOS(CEDULA,NOMBRE,APELLIDO,TELEFONO,FECHA_NACIMIENTO,VACUNA,PROVINCIA) VALUES(%s,%s,%s,%s,NOW(),%s,%s)"""
        campos = (cedula,nombre,apellido,telefono,vacuna,provincia)
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql,campos)
        connection.commit()
        return 1
    except:
        return 0


@app.post("/RegistrarVacuna")
def RegistroVacuna(nombre:str,cantidad:int):
    try:
        consulta = InsertVacuna(nombre,cantidad)
        if(consulta != 0):
            return {"Mensaje": "Vacuna registrada exitosamente"}
        else:
            return {"Fallo": "Hubo fallo en el insert"}
    except:
        return "Error en la operacion"

@app.post("/RegistrarProvincia")
def RegistroProvincia(nombre:str):
    try:
        consulta = InsertProvincia(nombre)
        if(consulta != 0):
            return {"Mensaje": "Provincia registrada exitosamente"}
        else:
            return {"Fallo": "Hubo fallo en el insert"}
    except:
        return "Error en la operacion"

@app.post("/RegistrarVacunado")
def RegistroVacunado(cedula:str,nombre:str,apellido:str,telefono:str,vacuna:int,provincia:int):
    try:
        consulta = InsertVacunado(cedula,nombre,apellido,telefono,vacuna,provincia)
        if(consulta != 0):
            return {"Mensaje": "Vacunado registrado exitosamente"}
        else:
            return {"Fallo": "Hubo fallo en el insert"}
    except:
        return "Error en la operacion"

@app.get("/Vacunados")
def ObtenerVacunos():
    try:
        global counter
        sql = """SELECT * FROM VACUNADOS"""
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        dictionary = {}
        _dictionary = {}
        counter = 0
        for k in result:
            _dictionary = dict(ID=k[0],CEDULA=k[1],NOMBRE=k[2],APELLIDO=k[3],TELEFONO=k[4],FECHA=k[5],VACUNA=k[6],PROVINCIA=k[7])
            dictionary.update({f"Vacunados{counter}": _dictionary})
            counter+=1
        return dict(Personas=dictionary)
    except:
        return dict(Error="Fallo la ejecucion")


@app.get("/VacunadosPorVacuna")
def ObtenerPorVacuna(vacuna:int):
    try:
        global counter
        sql = """SELECT * FROM VACUNADOS WHERE VACUNA = %s"""
        campos = (vacuna)
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql,campos)
        result = cursor.fetchall()
        dictionary = {}
        _dictionary = {}
        counter = 0
        for k in result:
            _dictionary = dict(ID=k[0],CEDULA=k[1],NOMBRE=k[2],APELLIDO=k[3],TELEFONO=k[4],FECHA=k[5],VACUNA=k[6],PROVINCIA=k[7])
            dictionary.update({f"VacunadosPorMarcaDeVacunas{counter}": _dictionary})
            counter+=1
        return dict(PorMarcaDeVacunas=dictionary)
    except:
        return dict(Error="Fallo la ejecucion")

@app.get("/VacunadosPorProvincia")
def ObtenerVacunosPorProvincia(provincia:int):
    try:
        global counter
        sql = """SELECT * FROM VACUNADOS WHERE PROVINCIA = %s"""
        campos = (provincia)
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql,campos)
        result = cursor.fetchall()
        dictionary = {}
        _dictionary = {}
        counter = 0
        for k in result:
            _dictionary = dict(ID=k[0],CEDULA=k[1],NOMBRE=k[2],APELLIDO=k[3],TELEFONO=k[4],FECHA=k[5],VACUNA=k[6],PROVINCIA=k[7])
            dictionary.update({f"VacunadosPorProvincia{counter}": _dictionary})
            counter+=1
        return dict(PorProvincia=dictionary)
    except:
        return dict(Error="Fallo la ejecucion")

@app.delete("/EliminarVacunado")
def EliminarVacunado(cedula:str):
    try:
        sql = """ DELETE FROM VACUNADOS WHERE CEDULA = %s """
        campos = (cedula)
        connection = myDB
        cursor = connection.cursor()
        cursor.execute(sql,campos)
        connection.commit()
        return {"Mensaje": "Se elimino un registro"}
    except:
        return {"Fallo": "Hubo fallo en el delete"}

def getZodiacalSign(date):
    sign = ['capricornio', 'acuario', 'piscis', 'aries', 'tauro', 'geminis', 'cancer', 'leo', 'virgo', 'libra', 'escorpio', 'sagitario']
    dates = [20, 19, 20, 20, 21, 21, 22, 22, 22, 22, 22, 21]
    day = int(date[8:10])
    month = (int(date[5:7])) - 1
    if day > dates[month]:
        month += 1
        if month == 12:
            month = 0
    return sign[month].upper()

def Zodiacal():
    try:
        connection = myDB
        myCursor = connection.cursor()
        sql = f"""SELECT ID, CEDULA, NOMBRE, APELLIDO, FECHA_NACIMIENTO FROM VACUNADOS"""
        myCursor.execute(sql)
        myResult = myCursor.fetchall()
        return [1, myResult]
    except:
        return [0]

@app.get("/zodiacal")
def _zodiacal():
    try:
        global ASaP
        condicion = Zodiacal()
        if (condicion[0] != 0):
            ASaP = int(0)
            dictionary = {}
            array = condicion[1]
            for k in array:
                date = str(k[4])
                zodiacalsign = getZodiacalSign(date)
                dictionary.update({
                    f"Persona{ASaP}": {
                        "ID": k[0],
                        "CEDULA": k[1],
                        "NOMBRE": k[2],
                        "APELLIDO": k[3],
                        "FECHA": date,
                        "SIGNO ZODIACAL": zodiacalsign
                    }
                })
                ASaP += 1
            return {
                "Patients": dictionary
                }
        else:
            return {
                "ERROR": "ERROR IN DATABASE"
            }
    except:
        return {
            "ERROR": "FASTAPI ERROR"
        }
