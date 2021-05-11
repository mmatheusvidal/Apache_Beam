import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

import re

pipeline = beam.Pipeline(options=PipelineOptions())

colunas = ['id', 'data_iniSE', 'casos', 'ibge_code', 'cidade', 'uf', 'cep', 'latitude', 'longitude']
colunas_chuva = ['data', 'mm', 'uf']

def list_to_dict(list, colunas):
    """
    Recebe lista e returna um dicionario
    """
    return dict(zip(colunas, list))

def text_to_list(line,sep='|'):
    """
    Recebe texto e separador e retorna uma lista
    """
    return line.split(sep)

def format_data(line):
    """
    Recebe um dicionario e cria um campo com ANO-MES em um novo campo
    """
    line['ano_mes'] = '-'.join(line['data_iniSE'].split('-')[:2])
    return line

def chave_uf_ano_mes_de_lista(line):
    """
    Recebe uma lista e cria uma tupla com chave ANO-MES e o valor da chuva em 'mm'
    ('UF-Ano_mes', mm)
    """
    data, mm, uf = line
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

def chave_uf(line):
    """
    Recebe um dicionario e retorna uma tupla (UF, dicionario)
    """
    chave = line['uf']
    return (chave, line)

def arredonda_mm(line):
    """
    Recebe tupla e arredonda o valor da chuva
    """
    chave, mm = line
    return (chave, round(mm, 1))

def casos_dengue(line):
    """
    Recebe a tupla e retorna uma tupla com o ano e soma de casos
    """
    uf, registros = line
    for i in registros:
        if bool(re.search(r'\d', i['casos'])):
            yield (f"{uf}-{i['ano_mes']}", float(i['casos']))
        else:
            yield (f"{uf}-{i['ano_mes']}", 0.0)

def filtra_campos_vazios(line):
    """
    Remove linhas que tem chave vazia
    Recebe ('CE-2015-08', {'chuvas': [0.0], 'dengue': [169.0]})
    Returna a mesma tupla sem os vazios
    """
    chave, dados = line
    if all([dados['chuvas'], dados['dengue']]):
        return True
    return False

def descompacta_tupla(line):
    """
    Recebe tupla ('CE-2015-12', {'chuvas': [7.6], 'dengue': [29.0]})
    Retorna tupla ('CE', '2015', '12', '7.6', '29.0')
    """
    chave, dados = line
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return (uf, ano, mes, str(chuva), str(dengue))

def prepara_csv(line, sep=';'):
    """
    Recebe uma tupla e retorna uma string delimitada (csv)
    """
    return f'{sep}'.join(line)

dengue = (
    pipeline
    | 'Leitura do Dataset: Dengue' >> ReadFromText('../Dados/casos_dengue.txt', skip_header_lines=1)
    | 'Texto para lista' >> beam.Map(text_to_list)
    | 'Lista para dicionario' >> beam.Map(list_to_dict, colunas)
    | 'Cria campo ano_mes' >> beam.Map(format_data)
    | 'Cria chave UF' >> beam.Map(chave_uf)
    | 'Agrupa por UF' >> beam.GroupByKey()
    | 'Cria chave UF-Ano_mes' >> beam.FlatMap(casos_dengue)
    | 'Soma dos casos pela chave' >> beam.CombinePerKey(sum)
    #| 'Print' >> beam.Map(print)
    )

chuvas = (
    pipeline
    | 'Leitura do Dataset: Chuvas' >> ReadFromText('../Dados/chuvas.csv', skip_header_lines=1)
    | 'Texto para Lista' >> beam.Map(text_to_list, sep=',')
    | 'Cria chave UF-ANO_MES' >> beam.Map(chave_uf_ano_mes_de_lista)
    | 'Soma do total de chuvas' >> beam.CombinePerKey(sum)
    | 'Arredonda resultado de chuvas' >> beam.Map(arredonda_mm)
    #| 'Print' >> beam.Map(print)
)

result = (
    #(chuvas, dengue)
    #| 'Empilha as PCols' >> beam.Flatten()
    #| 'Agrupamento' >> beam.GroupByKey()
    ({'chuvas': chuvas, 'dengue': dengue})
    | 'Mescla pcols' >> beam.CoGroupByKey()
    | 'Filtra dados vazios' >> beam.Filter(filtra_campos_vazios)
    | 'Descompacta elementos' >> beam.Map(descompacta_tupla)
    | 'Prepara CSV' >> beam.Map(prepara_csv)
    #| 'Printa Join' >> beam.Map(print)
)

header = 'Estado;Ano;Mes;Chuva;Dengue'
result | 'Cria arquivo CSV' >> WriteToText('../Dados/resultado', file_name_suffix='.csv', header=header)

result = pipeline.run()
result.wait_until_finish()
