import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import date, datetime
from supabase import create_client, Client
from PIL import Image
import streamlit as st

# Carrega a logo
logo = Image.open("frigard corel.png")

# Centraliza usando coluna vazia dos lados
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.image(logo, use_container_width=True)

# ===================== CONFIG =====================
st.set_page_config(page_title="Controle de Abate de Boi", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

ORIGENS = ["CONFINAMENTO", "PASTO", "ABATE DIRETO", "SEMI-CONFINAMENTO"]
SEXO_OPTS = ["M", "F"]
MESES = ["JANEIRO","FEVEREIRO","MARÇO","ABRIL","MAIO","JUNHO",
         "JULHO","AGOSTO","SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO"]

# ===================== HELPERS =====================
def ordenar_por_mes(df: pd.DataFrame, col="mes_nome") -> pd.DataFrame:
    """Ordena um DF por nome do mês (pt-BR). Aceita também numérico 1..12."""
    mapa = {m: i for i, m in enumerate(MESES, start=1)}
    ordem = df[col].map(mapa).fillna(pd.to_numeric(df[col], errors="coerce"))
    return df.assign(_ord=ordem).sort_values("_ord").drop(columns=["_ord"])

@st.cache_data(ttl=30)
def fetch_abates() -> pd.DataFrame:
    # Colunas esperadas em qualquer cenário
    cols_base = [
        "id","codigo","sexo","origem","destino",
        "data_entrada_confinamento","data_abate",
        "peso_entrada_kg","peso_abate_kg","rendimento_carcaca_pct",
        "created_at",
        "dias_confinado","ganho_peso_kg","gmd_kg_dia",
        "peso_carcaca_kg","@_arrobas",
        "ano","mes","mes_nome"
    ]

    try:
        resp = supabase.table("abates").select("*").execute()
        df = pd.DataFrame(resp.data or [])
    except Exception as e:
        # Se der qualquer erro de conexão, devolve DF vazio com as colunas
        return pd.DataFrame(columns=cols_base)

    if df.empty:
        return pd.DataFrame(columns=cols_base)

    # Tipagem
    for c in ["peso_entrada_kg","peso_abate_kg","rendimento_carcaca_pct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["data_entrada_confinamento","data_abate","created_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # Derivados
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

    # Tempo
    df["ano"] = pd.to_datetime(df["data_abate"], errors="coerce").dt.year
    df["mes"] = pd.to_datetime(df["data_abate"], errors="coerce").dt.month
    df["mes_nome"] = df["mes"].apply(lambda m: MESES[m-1] if pd.notna(m) else np.nan)

    # Garante todas as colunas
    for c in cols_base:
        if c not in df.columns:
            df[c] = np.nan

    return df[cols_base]

@st.cache_data(ttl=30)
def fetch_lojas() -> list[str]:
    """Busca lojas da tabela public.lojas; retorna lista ordenada."""
    resp = supabase.table("lojas").select("nome").order("nome").execute()
    return sorted([r["nome"] for r in (resp.data or [])])

def add_loja_if_new(nome: str):
    nome = (nome or "").strip()
    if not nome:
        return
    try:
        supabase.table("lojas").insert({"nome": nome}).execute()
        fetch_lojas.clear()   # limpa cache
    except Exception:
        pass

def upsert_abate(payload: dict):
    # Garante unicidade por (codigo, data_abate)
    sel = supabase.table("abates").select("id") \
        .eq("codigo", payload["codigo"]).eq("data_abate", payload["data_abate"]).execute()
    if sel.data:
        rec_id = sel.data[0]["id"]
        supabase.table("abates").update(payload).eq("id", rec_id).execute()
    else:
        supabase.table("abates").insert(payload).execute()
    fetch_abates.clear()  # recarrega cache

def delete_abate(codigo: str, data_abate: date):
    supabase.table("abates").delete().eq("codigo", codigo).eq("data_abate", str(data_abate)).execute()
    fetch_abates.clear()

def month_pivot(df, metric, agg="mean"):
    pt = df.pivot_table(index="origem", columns="mes_nome", values=metric,
                        aggfunc=("sum" if agg=="sum" else "mean"))
    pt = pt.reindex(columns=[c for c in MESES if c in pt.columns])
    pt = pt.reindex(index=[o for o in ORIGENS if o in pt.index])
    return pt

# ================== SIDEBAR (FILTROS) ==================
st.sidebar.title("Filtro")
df_all_cache = fetch_abates()

# Trata ausência de coluna ou valores de ano
anos_series = df_all_cache["ano"] if "ano" in df_all_cache.columns else pd.Series(dtype="float")
if anos_series.dropna().empty:
    anos = [datetime.now().year]
else:
    anos = sorted({int(x) for x in anos_series.dropna().unique()})

ano_sel = st.sidebar.selectbox("Ano do abate", anos, index=len(anos)-1)

origem_sel = st.sidebar.multiselect("Origem", ORIGENS, default=ORIGENS)

# Trata ausência de coluna destino
lojas_existentes = (
    sorted(df_all_cache["destino"].dropna().unique().tolist())
    if "destino" in df_all_cache.columns else []
)
dest_sel = st.sidebar.multiselect("Destino (loja)", lojas_existentes, default=lojas_existentes)

meses_sel = st.sidebar.multiselect("Meses", MESES, default=MESES)

# ================== TABS ==================
tab1, tab2, tab3 = st.tabs([
    "➕ Cadastro/Manutenção",
    "📊 Dashboard (Geral)",
    "📈 Tabelas & Indicadores",
])

# ----------------- TAB 1: CRUD ------------------
with tab1:
    st.subheader("Cadastrar/Editar Abate")

    with st.form("cadastro"):
        c1, c2, c3, c4 = st.columns(4)
        codigo = c1.text_input("Código *")
        sexo = c2.selectbox("Sexo *", SEXO_OPTS)
        origem = c3.selectbox("Origem *", ORIGENS)

        # ----- Seleção de loja (lendo do Supabase, com fallback e cadastro rápido) -----
        def get_lojas_options_for_form() -> list[str]:
            try:
                lojas = fetch_lojas()
            except Exception:
                lojas = []
            if not lojas:
                # fallback: usa lojas já lançadas nos abates
                lojas = sorted(fetch_abates()["destino"].dropna().unique().tolist())
            return lojas + ["+ Cadastrar nova loja..."]

        lojas_opts = get_lojas_options_for_form()
        loja_sel = c4.selectbox("Destino (loja) *", lojas_opts, index=0 if lojas_opts else None)
        destino = loja_sel
        if loja_sel == "+ Cadastrar nova loja...":
            destino = st.text_input("Digite o nome da nova loja").strip()
            if destino:
                add_loja_if_new(destino)  # grava na tabela lojas

        d1, d2 = st.columns(2)
        data_ent = d1.date_input("Data de **Entrada no Confinamento** (se houver)", value=None, format="DD/MM/YYYY")
        data_aba = d2.date_input("Data do **Abate** *", value=date.today(), format="DD/MM/YYYY")

        n1, n2, n3 = st.columns(3)
        peso_ent = n1.number_input("Peso de **Entrada** (kg)", min_value=0.0, step=1.0, format="%.0f")
        peso_abt = n2.number_input("Peso de **Abate** (kg)", min_value=0.0, step=1.0, format="%.0f")
        rend_pct = n3.number_input("**Rendimento de Carcaça** (%)", min_value=0.0, max_value=100.0, value=60.0, step=0.1)

        submitted = st.form_submit_button("Salvar registro")
        if submitted:
            if not (codigo and destino and data_aba and sexo and origem):
                st.error("Preencha os campos obrigatórios (*)")
            else:
                payload = {
                    "codigo": str(codigo).strip(),
                    "sexo": sexo,
                    "origem": origem,
                    "destino": destino.strip(),
                    "data_entrada_confinamento": str(data_ent) if data_ent else None,
                    "data_abate": str(data_aba),
                    "peso_entrada_kg": float(peso_ent) if peso_ent else None,
                    "peso_abate_kg": float(peso_abt) if peso_abt else None,
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
        f_or = colf3.multiselect("Filtrar por Origem", ORIGENS, default=ORIGENS)

        mask = df_view["origem"].isin(f_or)
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
            cod_del = st.text_input("Código exato do registro a excluir")
            data_del = st.date_input("Data do abate do registro a excluir", value=None, format="DD/MM/YYYY")
            if st.button("Excluir"):
                if cod_del and data_del:
                    delete_abate(cod_del, data_del)
                    st.warning("Registro(s) removido(s).")
                else:
                    st.error("Informe o código e a data do abate.")

# ----------------- APLICA FILTROS GLOBAIS ------------------
df_all = fetch_abates()
mask_global = (
    (df_all["ano"] == ano_sel) &
    (df_all["origem"].isin(origem_sel)) &
    (df_all["destino"].isin(dest_sel if dest_sel else df_all["destino"].unique())) &
    (df_all["mes_nome"].isin(meses_sel))
)
df = df_all.loc[mask_global].copy()

# ----------------- TAB 2: DASHBOARD ------------------
with tab2:
    st.subheader("Dashboard Geral")
    if df.empty:
        st.info("Sem dados para os filtros selecionados.")
    else:
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        total_animais = len(df)
        total_carcaca = df["peso_carcaca_kg"].sum(skipna=True)
        media_rend = df["rendimento_carcaca_pct"].mean(skipna=True)
        media_gmd = df["gmd_kg_dia"].mean(skipna=True)
        media_peso_carc = df["peso_carcaca_kg"].mean(skipna=True)
        media_peso_abate = df["peso_abate_kg"].mean(skipna=True)

        c1.metric("Animais abatidos", f"{total_animais:,}".replace(",", "."))
        c2.metric("Total de Carcaça (kg)", f"{total_carcaca:,.0f}".replace(",", "."))
        c3.metric("Média Rendimento de Carcaça", f"{(media_rend*100 if pd.notna(media_rend) else 0):.1f}%")
        c4.metric("Média GMD (kg/dia)", f"{media_gmd:.2f}" if pd.notna(media_gmd) else "-")
        c5.metric("Média Peso de Carcaça (kg)", f"{media_peso_carc:.0f}" if pd.notna(media_peso_carc) else "-")
        c6.metric("Média Peso Abate (kg)", f"{media_peso_abate:.0f}" if pd.notna(media_peso_abate) else "-")

        # Mensal
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
            st.plotly_chart(px.bar(df_m, x="mes_nome", y=df_m["media_rend"]*100,
                                   labels={"mes_nome":"Mês","y":"%"},
                                   title="MÉDIA DE RENDIMENTO DE CARCAÇA (%)"),
                            use_container_width=True)
        with colB:
            st.plotly_chart(px.bar(df_m, x="mes_nome", y="media_gmd",
                                   labels={"mes_nome":"Mês","media_gmd":"Kg/dia"},
                                   title="MÉDIA GMD (Kg/dia)"),
                            use_container_width=True)
        with colC:
            st.plotly_chart(px.bar(df_m, x="mes_nome", y="media_peso_carc",
                                   labels={"mes_nome":"Mês","media_peso_carc":"Kg"},
                                   title="MÉDIA DE PESO DE CARCAÇA (Kg)"),
                            use_container_width=True)

        colD, colE, colF = st.columns(3)
        with colD:
            st.plotly_chart(px.bar(df_m, x="mes_nome", y="media_peso_abate",
                                   title="MÉDIA PESO AO ABATE (Kg)"),
                            use_container_width=True)
        with colE:
            st.plotly_chart(px.bar(df_m, x="mes_nome", y="media_peso_entrada",
                                   title="MÉDIA DE PESO A ENTRADA (Kg)"),
                            use_container_width=True)
        with colF:
            st.plotly_chart(px.bar(df_m, x="mes_nome", y="soma_carcaca",
                                   title="TOTAL DE CARCAÇA (Kg)"),
                            use_container_width=True)

        st.markdown("##### Participações")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.plotly_chart(px.pie(df, names="origem", title="% ABATES por Origem"), use_container_width=True)
        with col2:
            st.plotly_chart(px.pie(df, names="destino", values="peso_carcaca_kg",
                                   title="Carcaça por Loja (kg)"), use_container_width=True)
        with col3:
            st.plotly_chart(px.pie(df, names="origem", title="Animais abatidos (qtd)"), use_container_width=True)

# ----------------- TAB 3: TABELAS/ÍNDICES ------------------
with tab3:
    st.subheader("Tabelas e Indicadores (por Origem x Mês)")
    if df.empty:
        st.info("Sem dados para os filtros selecionados.")
    else:
        st.markdown("**Abates mensais (Nº e % no mês)**")
        cont = df.pivot_table(index="origem", columns="mes_nome",
                              values="codigo", aggfunc="count").fillna(0).astype(int)
        cont = cont.reindex(columns=[c for c in MESES if c in cont.columns])
        cont = cont.reindex(index=[o for o in ORIGENS if o in cont.index])
        st.dataframe(cont, use_container_width=True)

        perc = (cont / cont.sum(axis=0) * 100).round(1).astype(str) + "%"
        st.dataframe(perc, use_container_width=True)
        st.caption("% por mês (coluna)")

        st.markdown("---")
        grids = [
            ("MÉDIA DE DIAS CONFINADO (dias)", "dias_confinado", "mean"),
            ("MÉDIA DE PESO A ENTRADA (kg)", "peso_entrada_kg", "mean"),
            ("MÉDIA PESO AO ABATE (kg)", "peso_abate_kg", "mean"),
            ("TOTAL DE CARCAÇA (kg)", "peso_carcaca_kg", "sum"),
            ("MÉDIA DE GANHO DE PESO (kg)", "ganho_peso_kg", "mean"),
            ("MÉDIA GMD (kg/dia)", "gmd_kg_dia", "mean"),
            ("MÉDIA DE PESO DE CARCAÇA (kg)", "peso_carcaca_kg", "mean"),
            ("PESO EQUIVALENTE (arrobas)", "@_arrobas", "mean"),
            ("MÉDIA DE RENDIMENTO DE CARCAÇA (%)", "rendimento_carcaca_pct", "mean"),
        ]
        for title, col, how in grids:
            st.markdown(f"**{title}**")
            pt = month_pivot(df, col, "sum" if how=="sum" else "mean")
            if col == "rendimento_carcaca_pct":
                st.dataframe((pt*100).round(1).astype(str) + "%", use_container_width=True)
            else:
                st.dataframe(pt.round(2), use_container_width=True)

        st.markdown("---")
        st.markdown("**Resumo Geral (médias por mês)**")
        df_m = df.groupby("mes_nome", as_index=False).agg(
            media_peso_abate=("peso_abate_kg","mean"),
            total_carcaca=("peso_carcaca_kg","sum"),
            media_peso_carc=("peso_carcaca_kg","mean"),
            arrobas_med=("@_arrobas","mean"),
            media_rendimento=("rendimento_carcaca_pct","mean"),
        )
        df_m["media_rendimento"] = (df_m["media_rendimento"]*100).round(1)
        df_m = ordenar_por_mes(df_m, "mes_nome")
        st.dataframe(df_m, use_container_width=True)

st.caption("✔️ Lance os abates na aba **Cadastro/Manutenção**. Os painéis e tabelas se atualizam automaticamente.")
