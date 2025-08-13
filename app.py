# ============================ APP FRIGORÍFICO ============================
# Dashboard & Cadastro de Abates (Streamlit + Supabase)

# ----------------------------- IMPORTS -----------------------------------
import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime
from supabase import create_client, Client
from PIL import Image
import plotly.express as px

# ------------------------ PAGE CONFIG (primeiro!) ------------------------
st.set_page_config(
    page_title="Controle de Abate de Boi",
    page_icon="🥩",
    layout="wide",
)

# ------------------------------- LOGO ------------------------------------
try:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # largura fixa para compatibilidade entre versões
        st.image("frigard corel.png", width=600)
except Exception:
    # se não tiver a imagem, não quebra
    pass

st.write("")  # espaçamento

# ----------------------------- CONSTANTES --------------------------------
ORIGENS = ["CONFINAMENTO", "PASTO", "ABATE DIRETO", "SEMI-CONFINAMENTO"]
SEXO_OPTS = ["M", "F"]
MESES = ["JANEIRO","FEVEREIRO","MARÇO","ABRIL","MAIO","JUNHO",
         "JULHO","AGOSTO","SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO"]

# --------------------------- SUPABASE CLIENT -----------------------------
SUPABASE_URL = st.secrets["SUPABASE_URL"]
# aceita SUPABASE_ANON_KEY (preferido) ou SUPABASE_KEY (fallback)
SUPABASE_KEY = st.secrets.get("SUPABASE_ANON_KEY", st.secrets.get("SUPABASE_KEY"))
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------------------------- HELPERS ---------------------------------
def _to_date(x):
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return pd.NaT

def ordenar_por_mes(df: pd.DataFrame, col="mes_nome") -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    df[col] = pd.Categorical(df[col], categories=MESES, ordered=True)
    return df.sort_values(col)

@st.cache_data(ttl=30)
def fetch_abates() -> pd.DataFrame:
    cols_base = [
        "id","codigo","sexo","origem","destino",
        "data_entrada_confinamento","data_abate",
        "peso_entrada_kg","peso_abate_kg","rendimento_carcaca_pct",
        "dias_confinado","ganho_peso_kg","gmd_kg_dia",
        "peso_carcaca_kg","@_arrobas",
        "ano","mes","mes_nome","created_at"
    ]
    try:
        resp = supabase.table("abates").select("*").execute()
        df = pd.DataFrame(resp.data or [])
    except Exception:
        return pd.DataFrame(columns=cols_base)

    if df.empty:
        return pd.DataFrame(columns=cols_base)

    # datas
    for c in ["data_entrada_confinamento","data_abate","created_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # numéricos
    for c in ["peso_entrada_kg","peso_abate_kg","rendimento_carcaca_pct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # derivados
    df["dias_confinado"] = np.where(
        df.get("data_entrada_confinamento").notna() & df.get("data_abate").notna(),
        (pd.to_datetime(df["data_abate"]) - pd.to_datetime(df["data_entrada_confinamento"])).dt.days,
        np.nan
    )
    df["ganho_peso_kg"] = np.where(
        df.get("peso_entrada_kg").notna() & df.get("peso_abate_kg").notna(),
        df["peso_abate_kg"] - df["peso_entrada_kg"], np.nan
    )
    df["gmd_kg_dia"] = np.where(
        (df["ganho_peso_kg"].notna()) & (df["dias_confinado"] > 0),
        df["ganho_peso_kg"] / df["dias_confinado"], np.nan
    )
    df["peso_carcaca_kg"] = np.where(
        df.get("peso_abate_kg").notna() & df.get("rendimento_carcaca_pct").notna(),
        df["peso_abate_kg"] * df["rendimento_carcaca_pct"], np.nan
    )
    df["@_arrobas"] = np.where(df["peso_carcaca_kg"].notna(), df["peso_carcaca_kg"]/15.0, np.nan)

    # tempo
    df["ano"] = pd.to_datetime(df["data_abate"], errors="coerce").dt.year
    df["mes"] = pd.to_datetime(df["data_abate"], errors="coerce").dt.month
    df["mes_nome"] = df["mes"].apply(lambda m: MESES[m-1] if pd.notna(m) else np.nan)

    # garante todas colunas
    for c in cols_base:
        if c not in df.columns:
            df[c] = np.nan

    return df[cols_base]

@st.cache_data(ttl=30)
def fetch_lojas() -> list[str]:
    try:
        resp = supabase.table("lojas").select("nome").order("nome").execute()
        return sorted([r["nome"] for r in (resp.data or [])])
    except Exception:
        return []

def add_loja_if_new(nome: str):
    nome = (nome or "").strip()
    if not nome:
        return
    try:
        supabase.table("lojas").insert({"nome": nome}).execute()
        fetch_lojas.clear()
    except Exception:
        pass

def upsert_abate(payload: dict):
    sel = supabase.table("abates").select("id") \
        .eq("codigo", payload["codigo"]).eq("data_abate", payload["data_abate"]).execute()
    if sel.data:
        supabase.table("abates").update(payload).eq("id", sel.data[0]["id"]).execute()
    else:
        supabase.table("abates").insert(payload).execute()
    fetch_abates.clear()

def delete_abate(codigo: str, data_abate: date):
    supabase.table("abates").delete().eq("codigo", codigo).eq("data_abate", str(data_abate)).execute()
    fetch_abates.clear()

def month_pivot(df, metric, agg="mean"):
    if df.empty:
        return pd.DataFrame()
    pt = df.pivot_table(index="origem", columns="mes_nome", values=metric,
                        aggfunc=("sum" if agg=="sum" else "mean"))
    # ordena colunas/linhas
    cols = [m for m in MESES if m in pt.columns]
    idx = [o for o in ORIGENS if o in pt.index]
    return pt.reindex(index=idx, columns=cols)

# ------------------------------- SIDEBAR ---------------------------------
st.sidebar.title("Filtro")
df_all_cache = fetch_abates()

anos_series = df_all_cache["ano"] if "ano" in df_all_cache.columns else pd.Series(dtype="float")
anos = sorted({int(x) for x in anos_series.dropna().unique()}) or [datetime.now().year]
ano_sel = st.sidebar.selectbox("Ano do abate", anos, index=len(anos)-1)

origem_sel = st.sidebar.multiselect("Origem", ORIGENS, default=ORIGENS)

lojas_existentes = (
    sorted(df_all_cache["destino"].dropna().unique().tolist())
    if "destino" in df_all_cache.columns else []
)
dest_sel = st.sidebar.multiselect("Destino (loja)", lojas_existentes, default=lojas_existentes)

meses_sel = st.sidebar.multiselect("Meses", MESES, default=MESES)

# --------------------------------- TABS ----------------------------------
tab1, tab2, tab3 = st.tabs([
    "➕ Cadastro/Manutenção",
    "📊 Dashboard (Geral)",
    "📈 Tabelas & Indicadores",
])

# ============================ TAB 1: CRUD ================================
with tab1:
    st.subheader("Cadastrar/Editar Abate")

    with st.form("cadastro"):
        c1, c2, c3, c4 = st.columns(4)
        codigo = c1.text_input("Código *")
        sexo = c2.selectbox("Sexo *", SEXO_OPTS)
        origem = c3.selectbox("Origem *", ORIGENS)

        def _lojas_options():
            lojas = fetch_lojas()
            if not lojas:
                lojas = sorted(fetch_abates()["destino"].dropna().unique().tolist())
            return lojas + ["+ Cadastrar nova loja..."]

        loja_opt = _lojas_options()
        destino = c4.selectbox("Destino (loja) *", loja_opt) if loja_opt else c4.text_input("Destino (loja) *")

        if destino == "+ Cadastrar nova loja...":
            destino = st.text_input("Nome da nova loja").strip()
            if destino:
                add_loja_if_new(destino)

        d1, d2 = st.columns(2)
        data_entrada = d1.date_input("Data de Entrada no Confinamento (se houver)", value=None, format="DD/MM/YYYY")
        data_abate = d2.date_input("Data do Abate *", value=date.today(), format="DD/MM/YYYY")

        n1, n2, n3 = st.columns(3)
        peso_ent = n1.number_input("Peso de Entrada (kg)", min_value=0.0, step=1.0, format="%.0f")
        peso_abt = n2.number_input("Peso de Abate (kg)", min_value=0.0, step=1.0, format="%.0f")
        rend_pct = n3.number_input("Rendimento de Carcaça (%)", min_value=0.0, max_value=100.0, value=60.0, step=0.1)

        submitted = st.form_submit_button("Salvar registro")
        if submitted:
            if not (codigo and destino and data_abate and sexo and origem):
                st.error("Preencha os campos obrigatórios (*)")
            else:
                payload = {
                    "codigo": str(codigo).strip(),
                    "sexo": sexo,
                    "origem": origem,
                    "destino": str(destino).strip(),
                    "data_entrada_confinamento": str(data_entrada) if data_entrada else None,
                    "data_abate": str(data_abate),
                    "peso_entrada_kg": float(peso_ent) if peso_ent else None,
                    "peso_abate_kg": float(peso_abt) if peso_abt else None,
                    # no banco guardamos como fração (0.60 = 60%)
                    "rendimento_carcaca_pct": float(rend_pct)/100.0 if rend_pct is not None else None,
                }
                upsert_abate(payload)
                st.success("Registro salvo/atualizado ✅")

    st.divider()
    st.subheader("Registros (filtro rápido)")

    df_view = fetch_abates()
    if df_view.empty:
        st.info("Sem registros ainda.")
    else:
        colf1, colf2, colf3 = st.columns(3)
        f_cod = colf1.text_input("Filtrar por Código contém", "")
        f_dest = colf2.text_input("Filtrar por Destino contém", "")
        f_org = colf3.multiselect("Filtrar por Origem", ORIGENS, default=ORIGENS)

        mask = df_view["origem"].isin(f_org)
        if f_cod:
            mask &= df_view["codigo"].astype(str).str.contains(f_cod, case=False, na=False)
        if f_dest:
            mask &= df_view["destino"].astype(str).str.contains(f_dest, case=False, na=False)

        st.dataframe(
            df_view.loc[mask, [
                "codigo","sexo","origem","destino","data_entrada_confinamento","data_abate",
                "dias_confinado","peso_entrada_kg","peso_abate_kg","ganho_peso_kg","gmd_kg_dia",
                "rendimento_carcaca_pct","peso_carcaca_kg","@_arrobas"
            ]].sort_values("data_abate", ascending=False),
            use_container_width=True
        )

        with st.expander("Excluir um registro"):
            cod_del = st.text_input("Código exato do registro")
            data_del = st.date_input("Data do abate desse registro", value=None, format="DD/MM/YYYY")
            if st.button("Excluir"):
                if cod_del and data_del:
                    delete_abate(cod_del, data_del)
                    st.warning("Registro(s) removido(s).")
                else:
                    st.error("Informe o código e a data do abate.")

# ------------ APLICA FILTROS GLOBAIS (para tabs 2 e 3) -------------------
df_all = fetch_abates()
mask_global = (
    (df_all["ano"] == ano_sel) &
    (df_all["origem"].isin(origem_sel)) &
    (df_all["mes_nome"].isin(meses_sel)) &
    (df_all["destino"].isin(dest_sel if dest_sel else df_all["destino"].unique()))
)
df = df_all.loc[mask_global].copy()

# ========================= TAB 2: DASHBOARD ==============================
with tab2:
    st.subheader("Dashboard Geral")
    if df.empty:
        st.info("Sem dados para os filtros selecionados.")
    else:
        # KPIs
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        total_animais = len(df)
        total_carcaca = df["peso_carcaca_kg"].sum(skipna=True)
        media_rend = df["rendimento_carcaca_pct"].mean(skipna=True)
        media_gmd = df["gmd_kg_dia"].mean(skipna=True)
        media_peso_carc = df["peso_carcaca_kg"].mean(skipna=True)
        media_peso_abate = df["peso_abate_kg"].mean(skipna=True)

        c1.metric("Animais abatidos", f"{total_animais:,}".replace(",", "."))
        c2.metric("Total de Carcaça (kg)", f"{total_carcaca:,.0f}".replace(",", "."))
        c3.metric("Média Rend. Carcaça", f"{(media_rend*100 if pd.notna(media_rend) else 0):.1f}%")
        c4.metric("Média GMD (kg/dia)", f"{media_gmd:.2f}" if pd.notna(media_gmd) else "-")
        c5.metric("Média Peso Carcaça (kg)", f"{media_peso_carc:.0f}" if pd.notna(media_peso_carc) else "-")
        c6.metric("Média Peso Abate (kg)", f"{media_peso_abate:.0f}" if pd.notna(media_peso_abate) else "-")

        # Agregações mensais
        df_m = df.groupby("mes_nome", as_index=False).agg(
            media_rend=("rendimento_carcaca_pct","mean"),
            media_gmd=("gmd_kg_dia","mean"),
            media_peso_carc=("peso_carcaca_kg","mean"),
            media_peso_abate=("peso_abate_kg","mean"),
            media_peso_entrada=("peso_entrada_kg","mean"),
            soma_carcaca=("peso_carcaca_kg","sum"),
            qtd=("codigo","count")
        )
        df_m = ordenar_por_mes(df_m, "mes_nome")

        colA, colB, colC = st.columns(3)
        with colA:
            st.plotly_chart(
                px.bar(df_m, x="mes_nome", y=df_m["media_rend"]*100,
                       labels={"mes_nome":"Mês","y":"%"},
                       title="MÉDIA DE RENDIMENTO DE CARCAÇA (%)"),
                use_container_width=True
            )
        with colB:
            st.plotly_chart(
                px.bar(df_m, x="mes_nome", y="media_gmd",
                       labels={"mes_nome":"Mês","media_gmd":"Kg/dia"},
                       title="MÉDIA GMD (Kg/dia)"),
                use_container_width=True
            )
        with colC:
            st.plotly_chart(
                px.bar(df_m, x="mes_nome", y="media_peso_carc",
                       labels={"mes_nome":"Mês","media_peso_carc":"Kg"},
                       title="MÉDIA DE PESO DE CARCAÇA (kg)"),
                use_container_width=True
            )

        colD, colE, colF = st.columns(3)
        with colD:
            st.plotly_chart(
                px.bar(df_m, x="mes_nome", y="media_peso_abate",
                       labels={"mes_nome":"Mês","media_peso_abate":"Kg"},
                       title="MÉDIA PESO AO ABATE (kg)"),
                use_container_width=True
            )
        with colE:
            st.plotly_chart(
                px.bar(df_m, x="mes_nome", y="media_peso_entrada",
                       labels={"mes_nome":"Mês","media_peso_entrada":"Kg"},
                       title="MÉDIA PESO À ENTRADA (kg)"),
                use_container_width=True
            )
        with colF:
            st.plotly_chart(
                px.bar(df_m, x="mes_nome", y="soma_carcaca",
                       labels={"mes_nome":"Mês","soma_carcaca":"Kg"},
                       title="TOTAL CARCAÇA (kg)"),
                use_container_width=True
            )

# ================= TAB 3: TABELAS & INDICADORES ==========================
with tab3:
    st.subheader("Tabelas e Indicadores")
    if df.empty:
        st.info("Sem dados para os filtros selecionados.")
    else:
        st.markdown("**Médias por Origem e Mês**")
        piv_rend = month_pivot(df, "rendimento_carcaca_pct", agg="mean")*100
        piv_gmd  = month_pivot(df, "gmd_kg_dia", agg="mean")
        piv_pcar = month_pivot(df, "peso_carcaca_kg", agg="mean")
        piv_pabt = month_pivot(df, "peso_abate_kg", agg="mean")
        piv_totc = month_pivot(df, "peso_carcaca_kg", agg="sum")

        with st.expander("Rendimento de Carcaça (%)"):
            st.dataframe(piv_rend.round(1), use_container_width=True)
        with st.expander("GMD (kg/dia)"):
            st.dataframe(piv_gmd.round(2), use_container_width=True)
        with st.expander("Peso de Carcaça (kg) - média"):
            st.dataframe(piv_pcar.round(0), use_container_width=True)
        with st.expander("Peso ao Abate (kg) - média"):
            st.dataframe(piv_pabt.round(0), use_container_width=True)
        with st.expander("Total de Carcaça (kg) - soma"):
            st.dataframe(piv_totc.round(0), use_container_width=True)
