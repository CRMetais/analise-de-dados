"""
essa é primeira q pega os dados do backend miguel

Consome GET /dashboard do backend e gera 3 arquivos CSV no S3:
  - info_fornecedor.csv
  - info_produto.csv
  - analise_variacao.csv
"""

import json
import csv
import io
import os
import urllib.request
import urllib.error
import boto3
from datetime import datetime, timezone


# ─── Config ───────────────────────────────────────────────────────────────────

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8080")
API_TOKEN   = os.environ.get("API_TOKEN", "")
S3_BUCKET   = os.environ.get("S3_BUCKET", "cr-metais-grafana")
S3_PREFIX   = os.environ.get("S3_PREFIX", "grafana/dados").rstrip("/")

s3_client = boto3.client("s3")


# ─── Fetch backend ────────────────────────────────────────────────────────────

def fetch_dashboard() -> dict:
    url = f"{BACKEND_URL}/dashboard"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type":  "application/json",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Backend retornou {e.code}: {e.reason}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Erro de conexao: {e.reason}")


# ─── Gerar CSVs ───────────────────────────────────────────────────────────────

def gerar_csv_info_fornecedor(rows: list) -> str:
    """
    Colunas: nome_fornecedor, ano, mes, peso_total, rendimento_total
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["nome_fornecedor", "ano", "mes", "peso_total", "rendimento_total"],
        lineterminator="\n"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({
            "nome_fornecedor":  row.get("nomeFornecedor"),
            "ano":              row.get("ano"),
            "mes":              row.get("mes"),
            "peso_total":       round(row.get("pesoTotal", 0) or 0, 4),
            "rendimento_total": round(row.get("rendimentoTotal", 0) or 0, 4),
        })
    return buffer.getvalue()


def gerar_csv_info_produto(rows: list) -> str:
    """
    Colunas: produto, ano, mes, rendimento_total, peso_total
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["produto", "ano", "mes", "rendimento_total", "peso_total"],
        lineterminator="\n"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({
            "produto":          row.get("produto"),
            "ano":              row.get("ano"),
            "mes":              row.get("mes"),
            "rendimento_total": round(row.get("rendimentoTotal", 0) or 0, 4),
            "peso_total":       round(row.get("pesoTotal", 0) or 0, 4),
        })
    return buffer.getvalue()


def gerar_csv_analise_variacao(rows: list) -> str:
    """
    Colunas: mes, media_preco, variacao_percentual
    variacao_percentual com 2 casas decimais e sinal % (ex: 3.15%)
    Primeiro mes fica vazio pois nao ha mes anterior para comparar.
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["mes", "media_preco", "variacao_percentual"],
        lineterminator="\n"
    )
    writer.writeheader()
    for row in rows:
        variacao   = row.get("variacaoPercentual")
        media      = row.get("mediaPreco")
        writer.writerow({
            "mes":                row.get("mes"),
            "media_preco":        round(media, 4) if media is not None else "",
            "variacao_percentual": f"{variacao:.2f}" if variacao is not None else "",
        })
    return buffer.getvalue()


# ─── Upload S3 ────────────────────────────────────────────────────────────────

def upload_s3(conteudo: str, nome_arquivo: str) -> str:
    chave = f"{S3_PREFIX}/{nome_arquivo}"
    s3_client.put_object(
        Bucket      = S3_BUCKET,
        Key         = chave,
        Body        = conteudo.encode("utf-8"),
        ContentType = "text/csv; charset=utf-8",
    )
    return f"s3://{S3_BUCKET}/{chave}"


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    try:
        print("Buscando dados do backend...")
        data = fetch_dashboard()

        info_fornecedor  = data.get("infoFornecedor", [])
        info_produto     = data.get("infoProduto", [])
        analise_variacao = data.get("analiseVariacao", [])

        print(
            f"Recebido: {len(info_fornecedor)} linhas fornecedor | "
            f"{len(info_produto)} linhas produto | "
            f"{len(analise_variacao)} linhas variacao"
        )

        # Gera os 3 CSVs em memória
        csv_fornecedor = gerar_csv_info_fornecedor(info_fornecedor)
        csv_produto    = gerar_csv_info_produto(info_produto)
        csv_variacao   = gerar_csv_analise_variacao(analise_variacao)

        # Upload no S3
        uri_fornecedor = upload_s3(csv_fornecedor, "info_fornecedor.csv")
        uri_produto    = upload_s3(csv_produto,    "info_produto.csv")
        uri_variacao   = upload_s3(csv_variacao,   "analise_variacao.csv")

        gerado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        resultado = {
            "sucesso":   True,
            "gerado_em": gerado_em,
            "arquivos": {
                "info_fornecedor":  uri_fornecedor,
                "info_produto":     uri_produto,
                "analise_variacao": uri_variacao,
            },
            "totais": {
                "linhas_fornecedor":  len(info_fornecedor),
                "linhas_produto":     len(info_produto),
                "linhas_variacao":    len(analise_variacao),
            }
        }

        print("Upload concluido:", json.dumps(resultado, ensure_ascii=False))

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(resultado, ensure_ascii=False),
        }

    except Exception as e:
        print(f"ERRO: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"sucesso": False, "erro": str(e)}),
        }


# ─── Teste local (sem S3) ─────────────────────────────────────────────────────

if __name__ == "__main__":
    resultado = lambda_handler({}, {})
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
    """
    Para testar localmente sem AWS, rode:
      BACKEND_URL=http://localhost:8080 API_TOKEN=seu_token python lambda_dashboard.py
    """
    import pprint

    print("Buscando dados do backend...")
    try:
        data = fetch_dashboard()

        print("\n=== info_fornecedor.csv (primeiras 5 linhas) ===")
        csv_f = gerar_csv_info_fornecedor(data.get("infoFornecedor", []))
        print("\n".join(csv_f.splitlines()[:6]))

        print("\n=== info_produto.csv (primeiras 5 linhas) ===")
        csv_p = gerar_csv_info_produto(data.get("infoProduto", []))
        print("\n".join(csv_p.splitlines()[:6]))

        print("\n=== analise_variacao.csv (primeiras 5 linhas) ===")
        csv_v = gerar_csv_analise_variacao(data.get("analiseVariacao", []))
        print("\n".join(csv_v.splitlines()[:6]))

        print("\nTotal linhas:")
        print(f"  fornecedor: {len(data.get('infoFornecedor', []))}")
        print(f"  produto:    {len(data.get('infoProduto', []))}")
        print(f"  variacao:   {len(data.get('analiseVariacao', []))}")

    except Exception as e:
        print(f"Erro: {e}")