from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine
from fastapi.routing import Request
import json
from sqlalchemy.exc import IntegrityError

app = FastAPI()

db_url = "postgresql://marin:marin@10.119.113.3:5432/postgres"
# db_url = "postgresql://marin:marin@35.247.250.121:5432/postgres"
perigo_spir_id = 0.005
tipo = {"spirid": "radiologico", "gdap": "quimico"}
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
    select {} from {} ORDER BY dt_created DESC {}
    """.format(
        colunas, table_name, limit
    )
    # Insere o DataFrame na tabela
    df = pd.read_sql(sql, con=conn)
    # Fecha a conexão
    conn.close()
    engine.dispose()

    return df


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


################################# Aqui começam as APIs #######################################
@app.get("/", tags=["Base"], summary="API de teste")
def home():
    return {"message": "PFC IME IDQBRN"}


@app.post(
    "/uploadSpirId",
    tags=["Carregamento de Dados"],
    summary="Carrega o csv gerado pelo SpirId",
    description="Carrega o CSV gerado pelo SPIR id exatamente como ele vem",
)
async def upload_csv(
    request: Request,
):
    json_data = await request.json()
    df = pd.DataFrame(json_data)
    colunas_spir_id = [
        "Time",
        "Mode",
        "State",
        "Warning 1",
        "Warning 2",
        "Longitude",
        "Latitude",
        "Heading",
        "Speed",
        "LGamma Dose rate (?Sv/h)",
        "LGamma BKG dose rate (?Sv/h)",
        "LGamma (cps)",
        "LGamma Bkg (cps)",
        "Neutron (cps)",
        "Neutron Bkg (cps)",
        "High range",
        "HGamma filtered (cps)",
        "HGamma filtered Dose rate (?Sv/h)",
        "Ext (cps)",
        "Event #",
        "Acq state",
        "Live time (s)",
        "Temperature(C)",
        "Dose (?Sv)",
        "MCA Satured",
        "ID 01",
        "CL 01",
        "ID 02",
        "CL 02",
        "ID 03",
        "CL 03",
        "ID 04",
        "CL 04",
        "ID 05",
        "CL 05",
        "ID 06",
        "CL 06",
        "ID 07",
        "CL 07",
        "ID 08",
        "CL 08",
        "Unnamed: 41",
    ]

    if list(df.columns) == colunas_spir_id:
        try:
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
            df["time"] = pd.to_datetime(df["time"]).dt.strftime('%Y-%m-%d %H:%M').astype(str)
            df.dropna(subset=["longitude"],inplace=True)
            df_base = df[["time", "state", "longitude", "latitude", "lgamma_dose_rate"]]
            df_base.dropna(subset=["longitude"],inplace=True)
            df_base["perigo"] = df["lgamma_dose_rate"] > perigo_spir_id
            df_base["tipoleitor"] = "radiologico"
            df_base["leitor"] = "SpirId"
            df_base = df_base.rename(
                columns={"time": "data", "state": "estado", "danger": "perigo"}
            )
            df_base = df_base[
                [
                    "data",
                    "tipoleitor",
                    "leitor",
                    "estado",
                    "longitude",
                    "latitude",
                    "perigo",
                    "lgamma_dose_rate"
                ]
            ]

            df_base = df_base.rename(
                columns={"lgamma_dose_rate": "leitura"}
            )

            df_base["leitura"] =  df_base["leitura"].astype(str) + " μSv/h"

            df_base = df_base.drop_duplicates(subset=['data', 'tipoleitor', 'leitor', 'estado', 'latitude', 'longitude'])
            df = df.drop_duplicates(subset= [
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
            ])
        except:
            return "Erro no processamento de dados"
        try:
            insert_dataframe_to_sql(df, "spirid")
            insert_dataframe_to_sql(df_base, "tabela_base")

            return "Sucesso"
        except:
            return "CSV com dados duplicados"

    else:
        return "CSV com formato não suportado"


@app.post(
    "/uploadIndividualRegister",
    tags=["Carregamento de Dados"],
    summary="Carrega dados inseridos manualmente",
    description="Carrega dados inseridos manualmente na pag inseriri dados na tabela ",
)
async def upload_csv(
    request: Request,
):
    raw_data = await request.json()
    json_data = {}
    if "data" in raw_data:
        json_data.update({"data": raw_data["data"]})
    if "estado" in raw_data:
        json_data.update({"estado": raw_data["estado"]})
    if "tipoleitor" in raw_data:
        json_data.update({"tipoleitor": raw_data["tipoleitor"]})
    if "leitor" in raw_data:
        json_data.update({"leitor": raw_data["leitor"]})
    if "longitude" in raw_data:
        json_data.update({"longitude": raw_data["longitude"]})
    if "latitude" in raw_data:
        json_data.update({"latitude": raw_data["latitude"]})
    if "leitura" in raw_data:
        json_data.update({"leitura": raw_data["leitura"]})

    dados = {}
    print((json_data["data"]))
    columns = [
        "data",
        "estado",
        "tipoleitor",
        "leitor",
        "longitude",
        "latitude",
        "leitura",
    ]
    print("data" in json_data)
    try:
        for item in columns:
            if str(item) in json_data:
                print(item)
                dados[item] = json_data[item]
                print("aqui")
            else:
                print("erro")
                return "Colunas do request erradas 1"

    except:
        return "Colunas do request erradas"

    if json_data["leitor"].lower() == "spirid":
        json_data["perigo"] = float(json_data["leitura"]) > perigo_spir_id
    else:
        json_data["perigo"] = False

    df_base = pd.DataFrame(data=json_data, index=[1])
    df_base.dropna(inplace=True)
    df_base["leitura"] = df_base["leitura"].astype(float)
    df_individual = df_base[
        ["data", "tipoleitor", "leitor", "estado", "longitude", "latitude", "leitura"]
    ]

    df_base = df_base[
        ["data", "tipoleitor", "leitor", "estado", "longitude", "latitude", "perigo","leitura"]
    ]

    if json_data["leitor"].lower() == "spirid":
        df_base["leitura"] = df_base["leitura"].astype(str) + " μSv/h"
    else:
        df_base["leitura"] = df_base["leitura"].astype(str) + " ppm"
    

    insert_dataframe_to_sql(df_individual, "registros_individuais")
    insert_dataframe_to_sql(df_base, "tabela_base")

    return True

############################ Leitura de dados ##########################
@app.get(
    "/statiticslast_100",
    tags=["Leitura de Dados"],
    summary="Alguns dados do SpirId",
    description="Retorna dados de leitura maxima, minima e ultima leitura para o sensor",
)
async def home(request: Request):
    df = read_staticts_last(
         "lgamma_dose_rate", "spirid", 100
    )
    
    return json.loads(df.to_json(orient="records", date_format="iso"))


@app.get(
    "/last_100",
    tags=["Leitura de Dados"],
    summary="Ultimos 100 dados adicionados as tabelas base",
    description="Retorna 100 dados mais recentes do sensor para preencher a tabela",
)
async def home():
    # try:
        df = read_table(
            ["data", "tipoleitor", "leitor","estado","latitude", "longitude", "perigo","leitura"],
            "tabela_base",
            100,
        )
        df = df.dropna()     
        return json.loads(df.to_json(orient="records", date_format="iso"))
    # except:
    #     return "Erro de leitura de dados"

@app.get(
    "/table/{model}",
    tags=["Leitura de Dados"],
    summary="Ultimos 100 dados do modelo",
    description="Retorna 100 dados mais recentes do sensor para preencher a tabela",
)
async def home(model: str = "spirid"):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"],
        "{}".format(model),
        100,
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

    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    df["Time"] = pd.to_datetime(df["Time"])
    return json.loads(df.to_json(orient="records", date_format="iso"))


@app.get(
    "/nr_leituras/{model}",
    tags=["Leitura de Dados"],
    summary="Retorna o numero total de leituras do modelo",
    description="Numero total de linhas na tabela para o modelo",
)
async def home(model: str):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"],
        "{}".format(model),
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

    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    leituras = len(df[df["Danger"] == True])
    return {"numero_leituras": leituras}


@app.get(
    "/perc_perigosas/{model}",
    tags=["Leitura de Dados"],
    summary="Retorna o percentual de leituras perigosas",
    description="Percentual das leituras que estão fora da faixa considerada segura para o spir id",
)
async def home(model: str):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"],
        "{}".format(model),
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

    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    porcentagem_perigo = round((len(df[df["Danger"] == True]) / len(df)) * 100, 2)
    return {"porcentagem_leituras_perigosas": porcentagem_perigo}


@app.get(
    "/info_leituras/{model}",
    tags=["Leitura de Dados"],
    summary="Retorna o percentual de leituras perigosas e nmr de leituras",
    description="Percentual das leituras que estão fora da faixa considerada segura",
)
async def home(model: str):
    df = read_table(
        ["time", "state", "latitude", "longitude", "lgamma_dose_rate"],
        "{}".format(model),
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

    df["TipoLeitor"] = tipo[model]
    df["Leitor"] = "{}".format(model)
    df["Danger"] = df["lgamma_dose_rate"] > perigo_spir_id
    df = df[
        ["Time", "TipoLeitor", "Leitor", "State", "Latitude", "Longitude", "Danger"]
    ]
    porcentagem_perigo = round((len(df[df["Danger"] == True]) / len(df)) * 100, 2)
    nr_leituras = len(df)
    return {
        "porcentagem_leituras_perigosas": porcentagem_perigo,
        "nr_leituras": nr_leituras,
    }


@app.get(
    "/table_month",
    tags=["Leitura de Dados"],
    summary="Retorna o percentual de leituras perigosas em cada mes com dados",
    description="Percentual das leituras que estão fora da faixa considerada segura em cada mes com dados na base de dados",
)
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
    df["mes"] = df["Time"].dt.month
    df["ano"] = df["Time"].dt.year

    grupo_mes_ano = df.groupby(["ano", "mes"])["Danger"].agg(
        tt_measures="size", danger_true="sum"
    )
    grupo_mes_ano["percentage_danger_true"] = (
        grupo_mes_ano["danger_true"] / grupo_mes_ano["tt_measures"]
    ) * 100
    grupo_mes_ano.reset_index(inplace=True)
    grupo_mes_ano["date"] = (
        grupo_mes_ano["ano"].astype(str) + grupo_mes_ano["mes"].astype(str) + "01"
    )

    grupo_mes_ano["date"] = pd.to_datetime(grupo_mes_ano["date"], format="%Y%m%d")
    grupo_mes_ano = grupo_mes_ano[
        ["date", "percentage_danger_true", "danger_true", "tt_measures"]
    ]

    return json.loads(grupo_mes_ano.to_json(orient="records", date_format="iso"))
