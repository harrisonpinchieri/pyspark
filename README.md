#Avaliação

Os exercícios foram realizados no databricks.

instalei a lib spark-xml_2.11-0.5.0.jar para conseguir gerar o xml.


Link do dataset: http://www.portaltransparencia.gov.br/download-de-dados/viagens




1) Ler CSV Viagens e gerar output Parquet
2) Ler CSV Viagens e gerar output XML
3) Ler CSV Viagens, ordenar por nome do órgão superior e nome do órgão solicitante em ordem decrescente. Gerar JSON do resultado.
4) Ler CSV Viagens e criar coluna "Valor Total" (Valor diária + Valor Passagens + Valor Outros)
5) Ler CSV e apresentar o total gasto por orgao solicitante onde a Situação for igual a Realizada (utilizar a coluna valor total que foi criada no item 4)
6) Ler CSV de viagens e de trechos. Selecionar os campos Id do processo de viagem, situação, nome do órgão superior, sequência trecho, destino - cidade. Salvar um JSON de outpout
