from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine
from fastapi.routing import Request
import json

app = FastAPI()

db_url = "postgresql://marin:marin@10.119.113.3:5432/postgres"
# db_url = "postgresql://marin:marin@35.247.250.121:5432/postgres"
perigo_spir_id = 0.005

################################### Funções auxiliares ######################################

def insert_dataframe_to_sql(dataframe: pd.DataFrame, table_name: str):
    # Cria uma conexão com o banco de dados
    engine = create_engine(db_url)
    conn = engine.connect()

    # Insere o DataFrame na tabela
    dataframe.to_sql(name=table_name, con=conn, if_exists="append", index=False)
    # Fecha a conexão
    conn.close()
    engine.dispose()


def read_table(columns: list, table_name: str, limit: int = ""):
    # Cria uma conexão com o banco de dados
    if limit != "":
        limit = "LIMIT {}".format(limit)
    colunas = ", ".join(columns)
    engine = create_engine(db_url)
    conn = engine.connect()
    sql = """
    select {} from {} ORDER BY time DESC {}
    """.format(
        colunas, table_name, limit
    )
    # Insere o DataFrame na tabela
    df = pd.read_sql(sql, con=conn)
    # Fecha a conexão
    conn.close()
    engine.dispose()

    return df

################################# Aqui começam as APIs #######################################
@app.get("/")
def home():
    return {"message": "PFC IME IDQBRN"}

def read_staticts_last(coluna: str, table_name: str, limit: int = ""):
    # Cria uma conexão com o banco de dados
    if limit != "":
        limit = "LIMIT {}".format(limit)
    
    engine = create_engine(db_url)
    conn = engine.connect()
    sql = """
    SELECT MAX(time) as data_ultima_afericao, AVG({}) AS media, MAX({}) AS valor_maximo, MIN({}) AS valor_minimo
    from ( select * from {}
    ORDER BY time DESC
    {}) A
    """.format(
        coluna,coluna,coluna,table_name,limit
    )
    # Insere o DataFrame na tabela
    df = pd.read_sql(sql, con=conn)
    # Fecha a conexão
    conn.close()
    engine.dispose()

    return df




@app.post("/uploadSpirId", summary="Carrega o csv gerado pelo SpirId", description="Carrega o CSV gerado pelo SPIR id exatamente como ele vem")
async def upload_csv(request: Request):
    json_data = await request.json()
    df = pd.DataFrame(json_data)
    df.drop("Unnamed: 41", axis=1, inplace=True)
    df.replace("--", None, inplace=True)
    df.columns = [
        "time",
        "mode",
        "state",
        "warning_1",
        "warning_2",
        "longitude",
        "latitude",
        "heading",
        "speed",
        "lgamma_dose_rate",
        "lgamma_bkg_dose_rate",
        "lgamma_cps",
        "lgamma_bkg_cps",
        "neutron_cps",
        "neutron_bkg_cps",
        "high_range",
        "hgamma_filtered_cps",
        "hgamma_filtered_dose_rate",
        "ext_cps",
        "event",
        "acq_state",
        "live_time_s",
        "temperature_c",
        "dose",
        "mca_satured",
        "id_01",
        "cl_01",
        "id_02",
        "cl_02",
        "id_03",
        "cl_03",
        "id_04",
        "cl_04",
        "id_05",
        "cl_05",
        "id_06",
        "cl_06",
        "id_07",
        "cl_07",
        "id_08",
        "cl_08",
    ]
    insert_dataframe_to_sql(df, "spirid")

    return True



@app.get("/statiticslast_100", summary="Alguns dados do SpirId", description="Retorna dados de leitura maxima, minima e ultima leitura para o sensor")
async def home(request: Request):
    df = read_staticts_last(
         "lgamma_dose_rate", "spirid", 100
    )
    
    return json.loads(df.to_json(orient="records", date_format="iso"))


@app.get("/table/{model}", summary="Ultimos 100 dados do modelo", description="Retorna 100 dados mais recentes do sensor para preencher a tabela")
async def home(model: str = "spirid"):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"], "{}".format(model), 100
    )
    df.rename(
        columns={
            "time": "Time",
            "state": "State",
            "latitude": "Latitude",
            "longitude": "Longitude",
        },
        inplace=True,
    )
    tipo = {
        "spirid":"radiologico",
        "gdap":"quimico"
    }
    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    df["Time"] = pd.to_datetime(df["Time"])
    return json.loads(df.to_json(orient="records", date_format="iso"))



@app.get("/nr_leituras/{model}", summary="Retorna o numero total de leituras do modelo", description="Numero total de linhas na tabela para o modelo")
async def home(model: str):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"], "{}".format(model)
    )
    df.rename(
        columns={
            "time": "Time",
            "state": "State",
            "latitude": "Latitude",
            "longitude": "Longitude",
        },
        inplace=True,
    )
    tipo = {
        "spirid":"radiologico",
        "gdap":"quimico"
    }
    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    leituras = (len(df[df["Danger"] == True]))
    return ({"numero_leituras":leituras})
@app.get("/perc_perigosas/{model}", summary="Retorna o percentual de leituras perigosas", description="Percentual das leituras que estão fora da faixa considerada segura")        
async def home(model: str):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"], "{}".format(model)
    )
    df.rename(
        columns={
            "time": "Time",
            "state": "State",
            "latitude": "Latitude",
            "longitude": "Longitude",
        },
        inplace=True,
    )
    tipo = {
        "spirid":"radiologico",
        "gdap":"quimico"
    }
    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    porcentagem_perigo = round((len(df[df["Danger"] == True])/len(df)) * 100,2)
    return ({"porcentagem_leituras_perigosas":porcentagem_perigo})

@app.get("/info_leituras/{model}", summary="Retorna o percentual de leituras perigosas e nmr de leituras", description="Percentual das leituras que estão fora da faixa considerada segura")
async def home(model: str):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"], "{}".format(model)
    )
    df.rename(
        columns={
            "time": "Time",
            "state": "State",
            "latitude": "Latitude",
            "longitude": "Longitude",
        },
        inplace=True,
    )
    tipo = {
        "spirid":"radiologico",
        "gdap":"quimico"
    }
    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    porcentagem_perigo = round((len(df[df["Danger"] == True])/len(df)) * 100,2)
    nr_leituras = len(df)
    return ({"porcentagem_leituras_perigosas":porcentagem_perigo,
             "nr_leituras":nr_leituras})


@app.get("/table_month", summary="Retorna o percentual de leituras perigosas em cada mes com dados", description="Percentual das leituras que estão fora da faixa considerada segura em cada mes com dados na base de dados")
async def home(request: Request):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"], "spirid"
    )
    df.rename(
        columns={
            "time": "Time",
            "state": "State",
            "latitude": "Latitude",
            "longitude": "Longitude",
        },
        inplace=True,
    )
    df["TipoLeitor"] = "Radiologico"
    df["Leitor"] = "SpirId"
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    df["Time"] = pd.to_datetime(df["Time"])
    df['mes'] = df["Time"].dt.month
    df['ano'] = df["Time"].dt.year

    grupo_mes_ano = df.groupby(['ano', 'mes'])['Danger'].agg(tt_measures='size', danger_true='sum')
    grupo_mes_ano['percentage_danger_true'] = (grupo_mes_ano['danger_true'] / grupo_mes_ano['tt_measures']) * 100
    grupo_mes_ano.reset_index(inplace=True)
    grupo_mes_ano['date'] = grupo_mes_ano['ano'].astype(str)  + grupo_mes_ano['mes'].astype(str) + '01'

    grupo_mes_ano["date"] = pd.to_datetime(grupo_mes_ano["date"], format='%Y%m%d')
    grupo_mes_ano = grupo_mes_ano[["date","percentage_danger_true","danger_true","tt_measures"]]

    return json.loads(grupo_mes_ano.to_json(orient="records", date_format="iso"))
