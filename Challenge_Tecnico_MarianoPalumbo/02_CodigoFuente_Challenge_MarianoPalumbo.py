import requests
import json
import csv
import datetime
import dateutil.parser
from datetime import datetime
from pathlib import Path

# <editor-fold desc="Variables">

# Rutas relativas de los recursos.
relative_path_mocks = Path("Resources/Mocks")
relative_path_credentials = Path("Resources/TokenCredentials")

# Endpoint
endpoint = 'https://api.mercadolibre.com/'

# Variables Internas para armar el output final.
data_dict = {}
dicts_final_output = []


# </editor-fold>

# <editor-fold desc="Token">

def get_token():
    resource = 'oauth/token'
    headers = {'accept': 'application/json',
               'content-type': 'application/x-www-form-urlencoded'}

    # Obtengo los datos para el body desde un arhivo de recursos para no exponer credenciales
    payload = read_file(make_relative_path(relative_path_credentials, 'Credentials.json'))

    response = requests.post(endpoint + resource, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        data = json.loads(response.text)
        return data["access_token"]


# </editor-fold>

# <editor-fold desc="Path">

# Funcion utilizada para crear paths .
def make_relative_path(relative_path, filename):
    return Path(relative_path, filename)


# </editor-fold>

# <editor-fold desc="File Reader">

# Funcion utilizada para la lectura de archivos, retorna en formato json.
def read_file(relative_path_and_filename):
    with open(relative_path_and_filename) as file:
        return json.load(file)


# </editor-fold>

# <editor-fold desc="API Calls">

# Llamado a API de Ordenes
def api_call_orders_by_order_id(order_id):
    headers = {"Authorization": 'Bearer ' + token}
    url = "https://api.mercadolibre.com/orders/{}".format(order_id)
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return json.loads(response.text)

# Llamado a API de envios
def api_call_shipments_by_shipping_id(shipping_id):
    headers = {"Authorization": 'Bearer ' + token, "X-Format-New": 'true'}
    url = "https://api.mercadolibre.com/shipments/{}".format(shipping_id)
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return json.loads(response.text)

# Llamado a API de Tiempos de Envio
def api_call_orders_lead_time(order_id):
    headers = {"Authorization": 'Bearer ' + token}
    url = "https://api.mercadolibre.com/orders/{}/shipments".format(order_id)
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return json.loads(response.text)

# Llamado a API de Carriers
def api_call_get_carrier_info_by_shipping_id(shipping_id):
    headers = {"Authorization": 'Bearer ' + token}
    url = "https://api.mercadolibre.com/shipments/{}/carrier".format(shipping_id)
    response = requests.get(url, headers=headers)

    if response.status_code == 200 or response.status_code == 404:
        return json.loads(response.text)


# </editor-fold>

# <editor-fold desc="Orchestrators">

# Funcion utilizada para obtener datos de las ordenes dependiendo del entorno.
def orchestrator_get_order_data(order_id_for_api, file_id_for_mock, enviroment):
    if (enviroment == "PROD"):
        return api_call_orders_by_order_id(order_id_for_api)
    else:
        return read_file(make_relative_path(relative_path_mocks, "{}_order.json".format(file_id_for_mock)))


# Funcion utilizada para obtener datos de los envios dependiendo del entorno.
def orchestrator_get_shipment_data(shipping_id_for_api, file_id_for_mock, enviroment):
    if (enviroment == "PROD"):
        return api_call_shipments_by_shipping_id(shipping_id_for_api)
    else:
        return read_file(make_relative_path(relative_path_mocks, "{}_shipment.json".format(file_id_for_mock)))


# Funcion utilizada para obtener datos de los tiempos de dependiendo del entorno.
def orchestrator_get_lead_time_data(order_id_for_api, file_id_for_mock, enviroment):
    if (enviroment == "PROD"):
        return api_call_orders_lead_time(order_id_for_api)
    else:
        return read_file(make_relative_path(relative_path_mocks, "{}_leadTime.json".format(file_id_for_mock)))


# Funcion utilizada para obtener datos del carrier dependiendo del entorno.
def orchestrator_get_carrier_data(shipping_id_for_api, file_id_for_mock, enviroment):
    if (enviroment == "PROD"):
        return api_call_get_carrier_info_by_shipping_id(shipping_id_for_api)
    else:
        return read_file(make_relative_path(relative_path_mocks, "{}_carrier.json".format(file_id_for_mock)))


# </editor-fold>

# <editor-fold desc="Parsers">

def order_parser(order):
    if (order is None):
        data_dict["Descripcion"] = "Error: Orden Nula"
        data_dict["Variation_Attributes"] = "Error: Orden Nula"
        data_dict["Monto Total de la Orden"] = "Error: Orden Nula"
        data_dict["Monto Total Abonado"] = "Error: Orden Nula"
        data_dict["Moneda utilizada"] = "Error: Orden Nula"
        return None

    else:

        count_lista_order_items= len(order["order_items"])
        count_lista_variation= len(order["order_items"][0]["item"]["variation_attributes"])

        if (count_lista_order_items==1): #Si es un solo item lo imprimo tal cual
            data_dict["Descripcion"] = order["order_items"][0]["item"]["title"]

            if(count_lista_variation!=0):

                variacion=""

                for a in range(0, len(order["order_items"][0]["item"]["variation_attributes"])):
                    variacion += "Id: " + str((order["order_items"][0]["item"]["variation_attributes"][a]["id"])) + \
                                 "  Name: " + str((order["order_items"][0]["item"]["variation_attributes"][a]["name"])) + \
                                 "  Value_Id: " + str((order["order_items"][0]["item"]["variation_attributes"][a]["value_id"])) + \
                                 "  Value_Name: " + str((order["order_items"][0]["item"]["variation_attributes"][a]["value_name"]))+ " / "

                data_dict["Variation_Attributes"] = variacion

        elif (count_lista_order_items>1): #Si es mas de un item imprimo concatenando con /
            str_concat_descripcion=""
            str_concat_variation=""

            for indice in range(0, len(order["order_items"])):
                str_concat_descripcion += (order["order_items"][indice]["item"]["title"]) + " / "

                for indice2 in range(0, len(order["order_items"][indice]["item"]["variation_attributes"])):
                    str_concat_variation += "Id: " + str((order["order_items"][indice]["item"]["variation_attributes"][indice2]["id"])) + \
                                            "  Name: " + str((order["order_items"][indice]["item"]["variation_attributes"][indice2]["name"])) + \
                                            "  Value_Id: " + str((order["order_items"][indice]["item"]["variation_attributes"][indice2]["value_id"])) + \
                                            "  Value_Name: " + str((order["order_items"][indice]["item"]["variation_attributes"][indice2]["value_name"])) + " / "

            data_dict["Descripcion"] = str_concat_descripcion
            data_dict["Variation_Attributes"] = str_concat_variation

        data_dict["Monto Total de la Orden"] = order["total_amount"]
        data_dict["Monto Total Abonado"] = order["paid_amount"]
        data_dict["Moneda utilizada"] = order["currency_id"]
        return order["shipping"]["id"]


def shipment_parser(shipment):
    if (shipment is None or shipment.get("logistic") == None):
        data_dict["Tipo de Logistica"] = "Sin datos de envio"
        data_dict["Estado del envio"] = "Sin datos de envio"
        data_dict["Sub Estado del envio"] = "Sin datos de envio"
        data_dict["Origen"] = "Sin Datos"
        data_dict["Destino"] = "Sin Datos"

    else:
        # Cual es el origen? Tipo logistica= fulfillment, entonces deposito propio sino, deposito=vendedor
        if (shipment["logistic"]["type"] == "fulfillment"):
            origin_deposit = "Deposito Propio"
        else:
            origin_deposit = "Deposito Vendedor"

        # Destino del envio: agency!=null, mostrar id agencia y id carrier sino mostrar domicilio completo
        if (shipment["destination"]["shipping_address"]["agency"]["agency_id"]) is None:
            destination = shipment["destination"]["shipping_address"]["address_line"]
        else:
            destination = "Id agencia: " + shipment["destination"]["shipping_address"]["agency"][
                "agency_id"] + " Id Carrier: " + shipment["destination"]["shipping_address"]["address_line"]

        data_dict["Tipo de Logistica"] = shipment["logistic"]["type"]
        data_dict["Estado del envio"] = shipment.get("status", "Sin datos")
        data_dict["Sub Estado del envio"] = shipment.get("substatus", "Sin datos")
        data_dict["Origen"] = origin_deposit
        data_dict["Destino"] = destination


def lead_time_parser(lead_time):
    if (lead_time is None or lead_time.get("status_history")==None):
        data_dict["Dia de Entrega"] = "Sin Datos"
        data_dict["Promesa de Entrega"] = "Sin Datos"
        data_dict["Fecha limite de entrega"] = "Sin Datos"
        return None

    else:
        str_fecha_de_entrega = lead_time["status_history"]["date_delivered"]
        str_fecha_prometida = lead_time["shipping_option"]["estimated_delivery_time"]["date"]
        str_fecha_limite_entrega = lead_time["shipping_option"]["estimated_delivery_limit"]["date"]

        data_dict["Dia de Entrega"] = str_fecha_de_entrega
        data_dict["Promesa de Entrega"] = str_fecha_prometida
        data_dict["Fecha limite de entrega"] = str_fecha_limite_entrega
        return (str_fecha_de_entrega, str_fecha_prometida, str_fecha_limite_entrega)


def carrier_parser(carrier):
    data_dict["Transportado por Proveedor"] = carrier.get("name", "Sin Informacion")


# </editor-fold>

# <editor-fold desc="Date Functions">

def convert_string_to_date(str_fecha):

    if (str_fecha is None or str_fecha == "null"):
        return None
    else:
        fecha_dt = datetime.strptime(str_fecha, "%Y-%m-%dT%H:%M:%S.%f%z")
        # String
        fecha_str = fecha_dt.strftime('%Y-%m-%d %H:%M:%S')
        # Date
        return dateutil.parser.parse(fecha_str)


# Verifica si el envio se cumple en tiempo y forma
def check_lead_time(fecha_entrega, fecha_prometida):
    if ((fecha_entrega is not None) and (fecha_prometida is not None)):

        if (fecha_entrega > fecha_prometida):
            return ("Entrega retrasada")

        else:
            return ("Entregado en tiempo y forma")

    else:
        return (
            "No fue posible calcular el tiempo del envio ya que la fecha de entega o la fecha prometida tienen valor nulo")


# Calcula el delay de entrega expresado en HH:mm:ss
def calculate_delay_time(fecha_entrega, fecha_prometida):
    if ((fecha_entrega is not None) and (fecha_prometida is not None)):

        dif_segundos = (fecha_entrega - fecha_prometida).total_seconds()

        # Por ejemplo si es 3665 segundos
        horas = str(int(dif_segundos // 3600))  # 1
        resto_segundos = int(dif_segundos % 3600)  # 65
        minutos = str(int(resto_segundos // 60))  # 1
        segundos_finales = str(int(resto_segundos % 60))  # 5

        return "{}:{}:{}".format(horas.zfill(2), minutos.zfill(2), segundos_finales.zfill(2))
    else:
        return (
            "No fue posible calcular el tiempo de delay ya que la fecha de entega o la fecha prometida tienen valor nulo")


# </editor-fold>

# <editor-fold desc="User-defined exceptions">

class EnvironmentInvalidException(Exception):
    """Dispara un error al inggresar un ambiente no válido"""
    pass


# </editor-fold>

# <editor-fold desc="GenerateCSV">

def generate_csv_file():
    labels = ['Descripcion', 'Variation_Attributes', 'Monto Total de la Orden', 'Monto Total Abonado',
              'Moneda utilizada', 'Tipo de Logistica', 'Estado del envio', 'Sub Estado del envio',
              'Origen', 'Destino', 'Transportado por Proveedor', 'Dia de Entrega', 'Promesa de Entrega',
              'Fecha limite de entrega', "Estado de la Entrega", "Delay de Entrega en HH:mm:ss"]

    try:
        with open('ResultadoEjecucion.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=labels)
            writer.writeheader()
            for elem in dicts_final_output:
                writer.writerow(elem)
    except IOError:
        print("Error al escribir arhivo. Compruebe que el recurso no se encuentre en uso")
        exit(1)


# </editor-fold>

# <editor-fold desc="Main">

# El usuario debe ingresar el entorno de ejecucion correspondiente. En vez de variable de entorno lo pido como input
enviroment = input("Ingrese el entorno de ejecucion [PROD] o [DEV] : ").upper().strip()

try:

    if (enviroment== "DEV" or enviroment == "PROD"):

        if (enviroment == "PROD"):
            print("Obteniendo Token de Seguridad...")
            # Obtengo el token de seguridad
            token = get_token()

        # Ordenes a consultar (son las ordenes de mi usuario de MeLi)
        order_list = ['5117516980','2211382141', '4828055354', '4761827347', '4105198596', '2222932100']

        print("Inicio del procesamiento, por favor aguarde...")
        for element in order_list:

            orden = orchestrator_get_order_data(element, element, enviroment)
            shipping_id = order_parser(orden)

            shipment = orchestrator_get_shipment_data(shipping_id, element, enviroment)
            shipment_parser(shipment)

            carrier = orchestrator_get_carrier_data(shipping_id, element, enviroment)
            carrier_parser(carrier)

            lead_time = orchestrator_get_lead_time_data(element, element, enviroment)
            fechas = lead_time_parser(lead_time)


            if (fechas is not None):
                str_fecha_de_entrega = fechas[0]
                str_fecha_prometida = fechas[1]
                str_fecha_limite_entrega = fechas[2]
            else:
                str_fecha_de_entrega = None
                str_fecha_prometida = None
                str_fecha_limite_entrega = None

            fecha_de_entrega = convert_string_to_date(str_fecha_de_entrega)
            fecha_prometida = convert_string_to_date(str_fecha_prometida)
            fecha_limite_entrega = convert_string_to_date(str_fecha_limite_entrega)

            data_dict["Estado de la Entrega"] = check_lead_time(fecha_de_entrega, fecha_prometida)

            # Calculo el tiempo de delay
            data_dict["Delay de Entrega en HH:mm:ss"] = calculate_delay_time(fecha_de_entrega, fecha_prometida)

            # Agrego registro al diccionario
            dicts_final_output.append((data_dict.copy()))

        # print(dicts_final_output)
        print("Generando archivo csv...")
        generate_csv_file()
        print("Procesamiento Finalizado!")

    else:
        raise EnvironmentInvalidException

except EnvironmentInvalidException:
    print("El entorno ingresado no es valido")
    exit(1)
except FileNotFoundError as ex:
    print("ERROR FATAL: Arhivo Inexistente: " + ex.filename + " (" + ex.strerror + ")")
    print("Se cancela ejecucion")
    exit(1)
except Exception as e:
    print("ERROR FATAL: " + str(e))
    exit(1)

# </editor-fold>
