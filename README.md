Sistema web em Python para cálculo de consumo de chapas e geração de planos de corte a partir de códigos de peças.

O software permite que o usuário informe o código da peça e a quantidade a ser cortada. A partir de uma planilha-base de peças, o sistema identifica automaticamente as medidas, material, espessura e tipo de chapa, calcula o consumo total de chapas, informa códigos não encontrados na base de dados e gera planos de corte por dois métodos: encaixe livre e guilhotina por faixas.

Principais funcionalidades:
- Entrada de peças por código e quantidade;
- Leitura automática da base de peças em Excel;
- Identificação de códigos não encontrados;
- Cálculo de consumo por tipo de material/chapa;
- Total de chapas utilizadas;
- Metragem quadrada cortada;
- Plano de corte por encaixe livre;
- Plano de corte por guilhotina/faixas;
- Consideração de kerf de serra de 4 mm;
- Rotação de peças em 90° permitida;
- Exportação de histórico em Excel;
- Geração de PDF/visualização dos planos de corte;
- Interface web simples, leve e acessível pelo navegador;
- Preparado para deploy em servidor web como Render.
