"""
AWS Lambda — CR Metais: Merge CSVs para Grafana

Lê do bucket cr-metais-grafana:
  - grafana/dados/info_fornecedor.csv
  - grafana/dados/info_produto.csv
  - grafana/dados/analise_variacao.csv

Lê do bucket cr-metais-base-externa:
  - BASE_COMMODITIES_TRATADA.csv

Gera no bucket cr-metais-grafana/grafana/dados/:
  - info_fornecedor.csv        (cópia direta)
  - info_produto.csv           (cópia direta)
  - analise_variacao_merged.csv (merge analise_variacao + commodities por data)

Variáveis de ambiente:
  BUCKET_DADOS    → bucket principal (default: cr-metais-grafana)
  PREFIX_DADOS    → prefixo dos CSVs gerados (default: grafana/dados)
  BUCKET_EXTERNO  → bucket da base externa (default: cr-metais-base-externa)
  KEY_EXTERNO     → chave do CSV externo (default: BASE_COMMODITIES_TRATADA.csv)
  BUCKET_DESTINO  → bucket de destino (default: cr-metais-grafana)
  PREFIX_DESTINO  → prefixo de destino (default: grafana/dados)
"""

import csv
import io
import os
import json
import boto3
from datetime import datetime, timezone

# ─── Config ───────────────────────────────────────────────────────────────────

BUCKET_DADOS   = os.environ.get("BUCKET_DADOS",   "cr-metais-grafana")
PREFIX_DADOS   = os.environ.get("PREFIX_DADOS",   "grafana/dados").rstrip("/")

BUCKET_EXTERNO = os.environ.get("BUCKET_EXTERNO", "cr-metais-base-externa")
KEY_EXTERNO    = os.environ.get("KEY_EXTERNO",    "BASE_COMMODITIES_TRATADA.csv")

BUCKET_DESTINO = os.environ.get("BUCKET_DESTINO", "cr-metais-grafana-final")
PREFIX_DESTINO = os.environ.get("PREFIX_DESTINO", "dados").rstrip("/")

s3 = boto3.client("s3")


# ─── S3 helpers ───────────────────────────────────────────────────────────────

def ler_csv_s3(bucket: str, key: str) -> list[dict]:
    """Lê um CSV do S3 e retorna lista de dicts."""
    print(f"Lendo s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    conteudo = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(conteudo))
    return list(reader)


def upload_csv_s3(rows: list[dict], fieldnames: list[str],
                  bucket: str, key: str) -> str:
    """Serializa lista de dicts como CSV e faz upload no S3 (sobrescreve)."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames,
                            lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = buffer.getvalue().encode("utf-8"),
        ContentType = "text/csv; charset=utf-8",
    )
    uri = f"s3://{bucket}/{key}"
    print(f"Upload concluido: {uri} ({len(rows)} linhas)")
    return uri


def copiar_csv_s3(bucket_origem: str, key_origem: str,
                  bucket_destino: str, key_destino: str) -> str:
    """Copia um objeto S3 para outro destino (sobrescreve)."""
    print(f"Copiando s3://{bucket_origem}/{key_origem} → s3://{bucket_destino}/{key_destino}")
    s3.copy_object(
        CopySource            = {"Bucket": bucket_origem, "Key": key_origem},
        Bucket                = bucket_destino,
        Key                   = key_destino,
        MetadataDirective     = "REPLACE",
        ContentType           = "text/csv; charset=utf-8",
    )
    return f"s3://{bucket_destino}/{key_destino}"


# ─── Normalização de data ─────────────────────────────────────────────────────

def normalizar_data(valor: str) -> str | None:
    """
    Aceita vários formatos e retorna sempre 'YYYY-MM-01'.
    Ex: '2024-01-01' → '2024-01-01'
        '2024-1-1'   → '2024-01-01'
    Retorna None se não conseguir parsear.
    """
    if not valor or not valor.strip():
        return None
    valor = valor.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(valor, fmt)
            # Sempre usa o dia 1 para garantir chave mensal uniforme
            return f"{dt.year}-{dt.month:02d}-01"
        except ValueError:
            continue
    return None


# ─── Merge analise_variacao + commodities ─────────────────────────────────────

def merge_variacao_commodities(
    rows_variacao: list[dict],
    rows_commodities: list[dict]
) -> tuple[list[dict], list[str]]:
    """
    Une analise_variacao.csv com BASE_COMMODITIES_TRATADA.csv pela coluna 'data' (YYYY-MM-01).

    analise_variacao colunas:    mes, media_preco, variacao_percentual
    commodities colunas:         NO_COMMODITIES, data, ano, mes, dia, Var_mensal

    Resultado: data, media_preco_cobre, variacao_cobre, variacao_commodities
    Mantém todos os meses presentes em analise_variacao.
    Preenche variacao_commodities com '' se não houver match.
    """

    # Indexa commodities por data normalizada
    commodities_map: dict[str, dict] = {}
    for row in rows_commodities:
        data_raw = row.get("data") or row.get("Data") or ""
        data_norm = normalizar_data(data_raw)
        if data_norm:
            commodities_map[data_norm] = row

    merged = []
    for row in rows_variacao:
        # normaliza a data vinda de analise_variacao (campo 'mes')
        data_norm = normalizar_data(row.get("mes", ""))
        if not data_norm:
            continue

        comm = commodities_map.get(data_norm, {})

        merged.append({
            "data":                    data_norm,
            "media_preco_cobre":       row.get("media_preco", ""),
            "variacao_cobre":          row.get("variacao_percentual", ""),
            "variacao_commodities":    comm.get("Var_mensal", ""),
        })

    fieldnames = ["data", "media_preco_cobre", "variacao_cobre", "variacao_commodities"]
    return merged, fieldnames


# ─── Handler ──────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    try:
        gerado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # ── 1. Copia info_fornecedor e info_produto diretamente ───────────────
        key_fornecedor_origem = f"{PREFIX_DADOS}/info_fornecedor.csv"
        key_produto_origem    = f"{PREFIX_DADOS}/info_produto.csv"
        key_variacao_origem   = f"{PREFIX_DADOS}/analise_variacao.csv"

        key_fornecedor_dest   = f"{PREFIX_DESTINO}/info_fornecedor.csv"
        key_produto_dest      = f"{PREFIX_DESTINO}/info_produto.csv"
        key_merged_dest       = f"{PREFIX_DESTINO}/analise_variacao_merged.csv"

        # Copia direto se origem == destino, senão faz copy normal
        if BUCKET_DADOS == BUCKET_DESTINO and key_fornecedor_origem == key_fornecedor_dest:
            uri_fornecedor = f"s3://{BUCKET_DESTINO}/{key_fornecedor_dest} (sem alteracao)"
            print(f"Origem == destino para info_fornecedor, pulando copia.")
        else:
            uri_fornecedor = copiar_csv_s3(
                BUCKET_DADOS, key_fornecedor_origem,
                BUCKET_DESTINO, key_fornecedor_dest
            )

        if BUCKET_DADOS == BUCKET_DESTINO and key_produto_origem == key_produto_dest:
            uri_produto = f"s3://{BUCKET_DESTINO}/{key_produto_dest} (sem alteracao)"
            print(f"Origem == destino para info_produto, pulando copia.")
        else:
            uri_produto = copiar_csv_s3(
                BUCKET_DADOS, key_produto_origem,
                BUCKET_DESTINO, key_produto_dest
            )

        # ── 2. Lê analise_variacao e BASE_COMMODITIES ─────────────────────────
        rows_variacao    = ler_csv_s3(BUCKET_DADOS,   key_variacao_origem)
        rows_commodities = ler_csv_s3(BUCKET_EXTERNO, KEY_EXTERNO)

        print(f"analise_variacao: {len(rows_variacao)} linhas")
        print(f"BASE_COMMODITIES: {len(rows_commodities)} linhas")

        # ── 3. Merge ──────────────────────────────────────────────────────────
        merged, fieldnames = merge_variacao_commodities(rows_variacao, rows_commodities)
        print(f"Merged: {len(merged)} linhas")

        # ── 4. Upload merged ──────────────────────────────────────────────────
        uri_merged = upload_csv_s3(merged, fieldnames, BUCKET_DESTINO, key_merged_dest)

        # ── 5. Resposta ───────────────────────────────────────────────────────
        resultado = {
            "sucesso":   True,
            "gerado_em": gerado_em,
            "arquivos": {
                "info_fornecedor":         uri_fornecedor,
                "info_produto":            uri_produto,
                "analise_variacao_merged": uri_merged,
            },
            "totais": {
                "linhas_variacao":    len(rows_variacao),
                "linhas_commodities": len(rows_commodities),
                "linhas_merged":      len(merged),
            }
        }

        print("Concluido:", json.dumps(resultado, ensure_ascii=False))

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(resultado, ensure_ascii=False),
        }

    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"sucesso": False, "erro": str(e)}),
        }


# ─── Teste local ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Teste local — precisa de credenciais AWS configuradas (aws configure).
    Rode: python lambda_merge.py
    """
    resultado = lambda_handler({}, None)
    print(f"\nStatus: {resultado['statusCode']}")
    import pprint
    pprint.pprint(json.loads(resultado["body"]))