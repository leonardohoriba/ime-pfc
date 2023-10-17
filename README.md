# ime-pfc

Conta utilizada:
email: idqbrnpfcime@gmail.com
senha: RicardoFranco2023

Output.json é um exemplo de csv do Spir Id 
# Pre requisitos:
É necessário tem instalado o docker 
## Autorizar o acesso ao banco de dados, caso queira acessalo fora do app 
1 - Acessa o google cloud
2 - Acessa SQL, conexões 
    https://console.cloud.google.com/sql/instances/idqbrn-ime/connections/summary?authuser=5&hl=pt-br&project=pfcime
3 - Aba Rede
4 - Cria autorização para o IP que deseja acessar 

## Para rodar o app 
0 - Crie um virtual env para instalar as dependencias

1 -  Inicialize o venv 

2 - execute 
    pip install requirements.txt -r 

3 - para rodar a aplicação 
    uvicorn main:app --reload

## Para testar rodar dentro de um conteiner docker local
Verifique se o deamon do docker está ligado 
Para rodar local:
db_url = "postgresql://marin:marin@35.247.250.121:5432/postgres"

1 - docker build -t gcr.io/pfcime/backend   .

2 - docker run -dp 8000:8000 -e PORT=8000 gcr.io/pfcime/backend
## Para dar deploy ao app no google cloud na conta utilizada durante o trabalho 
O pagamento da conta foi encerrado dia 20/10/2023, portanto, será necessário cadastro de nova forma de pagamento para 
que o ambiente em cloud volte a ficar ativo
Para rodar em nuvem:
db_url = "postgresql://marin:marin@10.119.113.3:5432/postgres"

1 - gcloud builds submit --tag gcr.io/pfcime/backend  

2 - gcloud run deploy backend --image gcr.io/pfcime/backend  --platform managed --region  southamerica-east1 --allow-unauthenticated