import pandas as pd
from google.oauth2 import service_account

# Conjunto de dados relativos aos contratos administrativos celebrados pela
# Agência Espacial Brasileira - AEB.
# Fonte:
# https://dados.gov.br/dataset/contratos-administrativos-celebrados-pela-agencia-espacial-brasileira-aeb


# ! Extração dos dados;
contracts = pd.read_csv("tabela_contratos.csv", sep=",")
contracts_dates = pd.read_csv("tabela_datas.csv", sep=",")
suppliers_data = pd.read_csv("tabela_empresas.csv", sep=",")


# ! Transformação dos dados;
contracts_and_suppliers = contracts.merge(
    suppliers_data,
    left_on="fk_empresa_contratada",
    right_on="id_empresa",
    how="left",
)

contracts_and_suppliers.drop(
    columns=["fk_empresa_contratada", "id_empresa"], inplace=True
)

contracts_data = contracts_and_suppliers.merge(
    contracts_dates,
    left_on="inicio_vigencia",
    right_on="id_data",
    how="left",
)

contracts_data.drop(columns=["inicio_vigencia", "id_data"], inplace=True)

contracts_data.rename(columns={"data": "data_inicio_vigencia"}, inplace=True)

contracts_final = contracts_data.merge(
    contracts_dates,
    left_on="termino_vigencia",
    right_on="id_data",
    how="left",
)

contracts_final.drop(columns=["termino_vigencia", "id_data"], inplace=True)

contracts_final.rename(columns={"data": "data_final_vigencia"}, inplace=True)

contracts_final["data_inicio_vigencia"] = contracts_final[
    "data_inicio_vigencia"
] = pd.to_datetime(contracts_final["data_inicio_vigencia"], format="%d/%m/%Y")

contracts_final["data_final_vigencia"] = contracts_final[
    "data_final_vigencia"
] = pd.to_datetime(contracts_final["data_final_vigencia"], format="%d/%m/%Y")

contracts_final["dias_de_contrato"] = (
    contracts_final["data_final_vigencia"]
    - contracts_final["data_inicio_vigencia"]
).dt.days

contracts_final = contracts_final[contracts_final["dias_de_contrato"] > 0]

contracts_final.reset_index(drop=True, inplace=True)


# ! Carregamento dos dados;
credentials = service_account.Credentials.from_service_account_file(
    filename="gbk.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

contracts_final.to_gbq(
    credentials=credentials,
    destination_table="sdw.aeb_contracts_csv",
    if_exists="replace",
)
