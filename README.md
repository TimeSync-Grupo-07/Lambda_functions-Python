# Lambda Functions - Development Guide

Este repositório contém o código-fonte das funções Lambda utilizadas na infraestrutura do projeto. Cada função deve estar contida em seu próprio diretório.

## Estrutura do Repositório

```shell
.
├── lambda_raw_function/
│ ├── lambda_function.py
│ └── requirements.txt
├── lambda_trusted_function/
│ ├── lambda_function.py
│ └── requirements.txt
├── README.md
```
## Como contribuir

1. Crie uma branch a partir da Dev:
   ```bash
   git checkout -b feature/sua-feature Dev

2. Edite o arquivo __lambda_function.py__ da função que deseja atualizar.

3. Se precisar de novas dependências, adicione no __requirements.txt__ correspondente.

4. Crie um Pull Request para a branch Dev.

## Deploy para Produção
O deploy das funções Lambda é feito de forma automatizada via Docker e Terraform, a partir da branch main.

O script de deploy empacota cada função Lambda com suas dependências num ambiente Linux compatível com a AWS, e envia o __.zip__ para o repositório de infraestrutura.

:::tip
    Você não precisa se preocupar com a compilação local, apenas manter os arquivos organizados.
::: 

## Boas práticas
- Sempre isole a lógica da função no lambda_function.py

- Use variáveis de ambiente para acessar buckets, bancos de dados, etc.

- Evite hardcoded (valores fixos dentro do código)