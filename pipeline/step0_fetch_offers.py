import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from src.shopee_affiliates_client import ShopeeAffiliatesClient


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUT_XLSX = os.path.join(DATA_DIR, "controle_produtos.xlsx")

QUERY_NAME = "productOfferV2"

LIMIT = int(os.getenv("STEP0_LIMIT", "50"))
MAX_PAGES = int(os.getenv("STEP0_MAX_PAGES", "1"))

KEYWORD = os.getenv("STEP0_KEYWORD", "").strip() or None
SORT_TYPE = int(os.getenv("STEP0_SORT_TYPE", "1"))

DESIRED_NODE_FIELDS = [
    "itemId", "itemid", "productId", "offerId", "id",
    "offerName", "productName", "title", "name",
    "offerLink", "productLink", "originalLink", "link",
    "imageUrl", "image_link", "image",
    "salePrice", "price", "priceMin", "priceMax", "originalPrice",
    "discountPercentage", "discountPercent", "discountRate",
    "categoryName", "category", "categoria", "categoryId",
    "rating", "itemRating", "shopRating", "reviewCount", "sold",
    "commissionRate",
    "shopId", "shopName", "offerType", "collectionId", "periodStartTime", "periodEndTime",
]

SCHEMA_QUERY = """
query SchemaAll {
  __schema {
    queryType {
      fields {
        name
        args {
          name
          type { kind name ofType { kind name ofType { kind name } } }
        }
        type { kind name ofType { kind name ofType { kind name } } }
      }
    }
  }
}
""".strip()

TYPE_FIELDS_QUERY = """
query TypeFields($typeName: String!) {
  __type(name: $typeName) {
    name
    kind
    fields {
      name
      type { kind name ofType { kind name ofType { kind name } } }
    }
  }
}
""".strip()


def _type_to_str(t: Dict[str, Any]) -> str:
    kind = t.get("kind")
    name = t.get("name")
    ofType = t.get("ofType")
    if kind == "NON_NULL":
        return _type_to_str(ofType) + "!"
    if kind == "LIST":
        return "[" + _type_to_str(ofType) + "]"
    return name or kind or "UNKNOWN"


def _unwrap_type_name(t: Dict[str, Any]) -> Optional[str]:
    while t and t.get("ofType") and t.get("kind") in ("NON_NULL", "LIST"):
        t = t.get("ofType")
    return t.get("name") if t else None


def _load_schema(client: ShopeeAffiliatesClient) -> List[Dict[str, Any]]:
    r = client.execute(SCHEMA_QUERY, variables={})
    data = r.get("data") if isinstance(r, dict) and "data" in r else r
    return data["__schema"]["queryType"]["fields"]


def _get_query_field(schema_fields: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    for f in schema_fields:
        if f.get("name") == name:
            return f
    raise RuntimeError(f"Query '{name}' não encontrada no schema. Disponíveis: {[x.get('name') for x in schema_fields]}")


def _introspect_type_fields(client: ShopeeAffiliatesClient, type_name: str) -> List[Dict[str, Any]]:
    r = client.execute(TYPE_FIELDS_QUERY, variables={"typeName": type_name})
    data = r.get("data") if isinstance(r, dict) and "data" in r else r
    t = data.get("__type")
    if not t or not t.get("fields"):
        return []
    return t["fields"]


def _detect_nodes_type(client: ShopeeAffiliatesClient, query_field: Dict[str, Any]) -> Tuple[str, str]:
    return_type = _unwrap_type_name(query_field["type"])
    if not return_type:
        raise RuntimeError("Não consegui identificar o tipo de retorno da query.")

    ret_fields = _introspect_type_fields(client, return_type)
    nodes_field = None
    for f in ret_fields:
        if f["name"] == "nodes":
            nodes_field = f
            break
    if not nodes_field:
        raise RuntimeError(f"Tipo '{return_type}' não possui campo 'nodes'.")

    nodes_type = _unwrap_type_name(nodes_field["type"])
    if not nodes_type:
        raise RuntimeError("Não consegui identificar o tipo de 'nodes'.")
    return return_type, nodes_type


def _pick_existing_fields(node_type_fields: List[Dict[str, Any]]) -> List[str]:
    existing = {f["name"] for f in node_type_fields}
    chosen = [f for f in DESIRED_NODE_FIELDS if f in existing]
    if not chosen:
        chosen = list(sorted(existing))[:15]
    return chosen


def _build_variables(args: List[Dict[str, Any]], page: int, limit: int) -> Tuple[Dict[str, Any], Dict[str, str]]:
    vars_payload: Dict[str, Any] = {}
    var_defs: Dict[str, str] = {}

    arg_names = {a["name"]: a for a in args}

    def put(name: str, value: Any):
        a = arg_names.get(name)
        if not a:
            return
        var_defs[name] = _type_to_str(a["type"])
        vars_payload[name] = value

    put("page", page)
    put("limit", limit)
    put("keyword", KEYWORD)
    put("sortType", SORT_TYPE)

    put("pageNo", page)
    put("pageNum", page)
    put("pageIndex", page)

    put("pageSize", limit)
    put("size", limit)

    put("search", KEYWORD)
    put("query", KEYWORD)

    vars_payload = {k: v for k, v in vars_payload.items() if v is not None}
    return vars_payload, var_defs


def _build_query(query_name: str, var_defs: Dict[str, str], node_fields: List[str]) -> str:
    defs = ""
    if var_defs:
        defs = "(" + ", ".join([f"${k}: {v}" for k, v in var_defs.items()]) + ")"

    call_args = ""
    if var_defs:
        call_args = "(" + ", ".join([f"{k}: ${k}" for k in var_defs.keys()]) + ")"

    fields_block = "\n      ".join(node_fields)

    return f"""
query {query_name}{defs} {{
  {query_name}{call_args} {{
    nodes {{
      {fields_block}
    }}
    pageInfo {{
      page
      limit
      hasNextPage
    }}
  }}
}}
""".strip()


def _normalize_nodes(nodes: List[Dict[str, Any]]) -> pd.DataFrame:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    def first(n: Dict[str, Any], *keys):
        for k in keys:
            if k in n and n.get(k) not in (None, "", "nan"):
                return n.get(k)
        return None

    for n in nodes:
        produto_id = first(n, "itemId", "itemid", "productId", "offerId", "id")
        nome = first(n, "offerName", "productName", "title", "name")
        link = first(n, "productLink", "offerLink", "originalLink", "link")
        img = first(n, "imageUrl", "image_link", "image")
        preco = first(n, "salePrice", "priceMin", "price", "priceMax", "originalPrice")
        rating = first(n, "rating", "itemRating", "shopRating")
        categoria = first(n, "categoryName", "category", "categoria", "categoryId")

        row = {
            "produto_id": produto_id,
            "nome_curto": nome,
            "link_afiliado": link,
            "preco_atual": preco,
            "avaliacao": rating,
            "categoria": categoria,
            "imageUrl": img,
            "ingested_at": now,
            "source": QUERY_NAME,
        }

        for k in ["commissionRate", "discountPercentage", "discountPercent", "discountRate", "offerType", "collectionId", "periodStartTime", "periodEndTime", "shopId", "shopName"]:
            if k in n:
                row[k] = n.get(k)

        rows.append(row)

    return pd.DataFrame(rows)


def _upsert_excel(path_xlsx: str, df_new: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(path_xlsx), exist_ok=True)
    key = "link_afiliado"

    if os.path.exists(path_xlsx):
        df_old = pd.read_excel(path_xlsx)
        if key in df_old.columns:
            df = pd.concat([df_old, df_new], ignore_index=True)
            df[key] = df[key].astype(str).fillna("").str.strip()
            df = df[df[key].str.len() > 0]
            df = df.drop_duplicates(subset=[key], keep="last")
        else:
            df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df.to_excel(path_xlsx, index=False)


def main():
    client = ShopeeAffiliatesClient.from_env()

    schema_fields = _load_schema(client)
    qf = _get_query_field(schema_fields, QUERY_NAME)
    args = qf.get("args") or []

    ret_type, nodes_type = _detect_nodes_type(client, qf)
    node_fields_meta = _introspect_type_fields(client, nodes_type)
    chosen_fields = _pick_existing_fields(node_fields_meta)

    all_nodes: List[Dict[str, Any]] = []
    page = 1
    removed = 0

    while page <= MAX_PAGES:
        variables, var_defs = _build_variables(args, page=page, limit=LIMIT)
        query = _build_query(QUERY_NAME, var_defs=var_defs, node_fields=chosen_fields)

        result = client.execute(query, variables=variables)
        data = result.get("data") if isinstance(result, dict) and "data" in result else result

        payload = data.get(QUERY_NAME)
        if not payload:
            raise RuntimeError(f"Resposta não contém '{QUERY_NAME}'. Keys: {list(data.keys()) if isinstance(data, dict) else None}")

        nodes = payload.get("nodes") or []
        page_info = payload.get("pageInfo") or {}
        all_nodes.extend(nodes)

        has_next = bool(page_info.get("hasNextPage"))
        if not has_next:
            break
        page += 1

    df_new = _normalize_nodes(all_nodes)

    if "link_afiliado" in df_new.columns:
        df_new["link_afiliado"] = df_new["link_afiliado"].astype(str).fillna("").str.strip()
        before = len(df_new)
        df_new = df_new[df_new["link_afiliado"].str.len() > 0]
        removed += (before - len(df_new))

    _upsert_excel(OUT_XLSX, df_new)

    print(f"OK: {len(df_new)} ofertas processadas. Excel atualizado em: {OUT_XLSX}")
    print(f"INFO Step0: query usada: {QUERY_NAME}")
    print(f"INFO Step0: return_type={ret_type} | nodes_type={nodes_type}")
    print(f"INFO Step0: campos em nodes usados ({len(chosen_fields)}): {chosen_fields}")
    if removed:
        print(f"INFO Step0: removidos {removed} itens sem link.")


if __name__ == "__main__":
    main()
