# APP CHAPAS - DUAS OPÇÕES DE CORTE

Esta versão atualiza a tela **Planos de corte** para trabalhar com dois modos:

1. **Encaixe livre**
   - Procura encaixar peças nos espaços livres da chapa.
   - Pode melhorar o aproveitamento em alguns lotes.
   - A sequência de corte é mais visual, não necessariamente a sequência mais simples para seccionadora.

2. **Guilhotina por faixas**
   - Organiza a chapa em faixas.
   - Gera uma sequência operacional mais clara para seccionadora.
   - Pode usar um pouco mais de chapa que o encaixe livre, mas tende a ser mais fácil de executar na operação.

Configuração padrão:
- Chapa sem sentido de veio.
- Peças podem girar 90 graus.
- Serra/kerf: 4 mm.
- Meta de aproveitamento: 95%.

## Como instalar no seu projeto atual

Se você já tem seu banco atualizado com produtos e peças, **não substitua o seu consumo_chapas.db**.

Faça assim:
1. Baixe o novo arquivo `app.py`.
2. Substitua apenas o `app.py` antigo pelo novo.
3. Mantenha seu `consumo_chapas.db` atual na mesma pasta.
4. Execute o sistema novamente.

## Como rodar localmente

No Windows:
1. Extraia os arquivos em uma pasta.
2. Dê dois cliques em `executar.bat`.
3. Acesse no navegador: `http://127.0.0.1:8000`

Login padrão:
- Usuário: `admin`
- Senha: `troque-esta-senha`

## Como usar os dois modos

Entre no menu:
`Planos de corte`

Escolha:
- `Encaixe livre`, ou
- `Guilhotina por faixas`

Informe:
- Número do lote.
- Produto.
- Quantidade.

Clique em:
`Gerar plano de corte`

O sistema exibirá:
- Quantidade de chapas.
- Aproveitamento real.
- Sobra em m².
- Desenho chapa por chapa.
- Sequência operacional sugerida.
- Lista técnica das peças.
- Botão para baixar PDF.

## Uso online

Para usar em Render ou outro servidor:
- envie os arquivos para o GitHub;
- configure o Web Service;
- se usar banco dentro do servidor, use disco persistente;
- defina `DATA_DIR=/var/data` quando usar persistent disk.

Variáveis importantes:
- `APP_USER`
- `APP_PASSWORD`
- `AUTH_ENABLED=1`
- `DATA_DIR=/var/data`
- `KERF_MM=4`
- `META_APROVEITAMENTO=0.95`
