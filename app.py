# app.py
from __future__ import annotations

from datetime import date, datetime
from io import BytesIO

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from supabase import Client, create_client

# ---------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# ---------------------------------------------------
st.set_page_config(
    page_title="Controle de Abate de Boi",
    page_icon="🐂",
    layout="wide",
)

# ---------------------------------------------------
# LOGO (sidebar + topo)
# ---------------------------------------------------
try:
    st.sidebar.image("frigard corel.png", use_column_width=True)
except Exception:
    pass

try:
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.image("frigard corel.png", use_column_width=True)
except Exception:
    pass

st.write("")

# ---------------------------------------------------
# DEPENDÊNCIA OPCIONAL (xlsxwriter para Excel)
# ---------------------------------------------------
try:
    import xlsxwriter  # noqa
    HAS_XLSXWRITER = True
except Exception:
    HAS_XLSXWRITER = False

# ---------------------------------------------------
# CONSTANTES
# ---------------------------------------------------
ORIGENS = ["CONFINAMENTO", "PASTO", "ABATE DIRETO", "SEMI-CONFINAMENTO"]
SEXO = ["M", "F"]
MESES = [
    "JANEIRO", "FEVEREIRO", "MARÇO", "ABRIL", "MAIO", "JUNHO",
    "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO",
]
MAPA_MES = {i + 1: nome for i, nome in enumerate(MESES)}

# ---------------------------------------------------
# SUPABASE
# ---------------------------------------------------
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets.get("SUPABASE_ANON_KEY", st.secrets.get("SUPABASE_KEY"))
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------
def ordenar_por_mes(df: pd.DataFrame, col: str = "mes_nome") -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.Categorical(df[col], categories=MESES, ordered=True)
        df = df.sort_values(col)
    return df


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def make_excel_workbook(df_registros: pd.DataFrame,
                        sheets: dict[str, pd.DataFrame] | None = None) -> BytesIO:
    """Gera Excel com aba Registros + abas extras (pivôs), com ajuste de largura robusto."""
    output = BytesIO()

    if not HAS_XLSXWRITER:
        output.write(b"")
        output.seek(0)
        return output

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # --- AQUI ESTÁ A CORREÇÃO ---
        if df_registros is None or not isinstance(df_registros, pd.DataFrame) or df_registros.empty:
            safe = pd.DataFrame({"(sem dados)": [""]})
        else:
            safe = df_registros.copy()
        # -----------------------------

        # Datas como texto
        for c in ["data_entrada_confinamento", "data_abate", "created_at"]:
            if c in safe.columns:
                safe[c] = pd.to_datetime(safe[c], errors="coerce").dt.strftime("%d/%m/%Y")

        # Aba principal
        safe.to_excel(writer, sheet_name="Registros", index=False)

        # Abas extras (pivôs)
        if sheets:
            for name, dfp in sheets.items():
                if isinstance(dfp, pd.DataFrame) and not dfp.empty:
                    dfp.to_excel(writer, sheet_name=name[:31], index=True)

        # Ajuste seguro de largura
        ws0 = writer.sheets["Registros"]
        for i, col in enumerate(safe.columns):
            try:
                s = safe[col].astype(str).replace({"nan": ""})
                lens = s.str.len()
                if lens.dropna().empty:
                    q = 10
                else:
                    q = lens.fillna(0).quantile(0.9)
                    if pd.isna(q):
                        q = 10
                width = max(10, min(35, int(q) + 2))
            except Exception:
                width = 15
            ws0.set_column(i, i, width)

    output.seek(0)
    return output

def add_loja_if_new(nome_loja: str):
    nome_loja = (nome_loja or "").strip()
    if not nome_loja:
        return
    try:
        existing = supabase.table("lojas").select("id").eq("nome", nome_loja).execute()
        if not (existing.data or []):
            supabase.table("lojas").insert({"nome": nome_loja}).execute()
    except Exception:
        pass


def month_pivot(df: pd.DataFrame, metric: str, agg: str = "mean") -> pd.DataFrame:
    if df.empty or metric not in df.columns:
        return pd.DataFrame()
    pt = df.pivot_table(index="origem", columns="mes_nome",
                        values=metric, aggfunc=("sum" if agg == "sum" else "mean"))
    cols = [m for m in MESES if m in pt.columns]
    idx = [o for o in ORIGENS if o in pt.index]
    return pt.reindex(index=idx, columns=cols)


def log_usage(action: str, rows: int = 0, notes: dict | None = None):
    """Logger opcional em public.usage_events (ignora erros se não existir)."""
    try:
        supabase.table("usage_events").insert({
            "action": action,
            "rows_affected": rows,
            "notes": notes or {}
        }).execute()
    except Exception:
        pass


# ---------------------------------------------------
# DADOS (fetch_abates com meses sem locale + novos cálculos)
# ---------------------------------------------------
@st.cache_data(ttl=20)
def fetch_abates() -> pd.DataFrame:
    res = supabase.table("abates").select("*").order("data_abate").execute()
    df = pd.DataFrame(res.data or [])

    # Datas -> date
    for c in ["data_entrada_confinamento", "data_abate", "created_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # Numéricos
    for c in ["peso_entrada_kg", "peso_abate_kg", "peso_carcaca_kg", "rendimento_carcaca_pct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    dt_ent = pd.to_datetime(df.get("data_entrada_confinamento"), errors="coerce")
    dt_aba = pd.to_datetime(df.get("data_abate"), errors="coerce")

    # Dias confinado
    df["dias_confinado"] = np.where(
        (dt_ent.notna()) & (dt_aba.notna()),
        (dt_aba - dt_ent).dt.days, np.nan
    )

    # ---- NOVO: peso de carcaça e rendimento (consistência)
    tem_abate = df.get("peso_abate_kg").notna()

    mask_calc_pct = tem_abate & df.get("peso_carcaca_kg").notna()
    df.loc[mask_calc_pct, "rendimento_carcaca_pct"] = (
        df.loc[mask_calc_pct, "peso_carcaca_kg"] / df.loc[mask_calc_pct, "peso_abate_kg"]
    )

    mask_calc_peso = tem_abate & df.get("rendimento_carcaca_pct").notna() & df.get("peso_carcaca_kg").isna()
    df.loc[mask_calc_peso, "peso_carcaca_kg"] = (
        df.loc[mask_calc_peso, "peso_abate_kg"] * df.loc[mask_calc_peso, "rendimento_carcaca_pct"]
    )

    # Ganho e GMD
    df["ganho_peso_kg"] = np.where(
        df.get("peso_abate_kg").notna() & df.get("peso_entrada_kg").notna(),
        df["peso_abate_kg"] - df["peso_entrada_kg"], np.nan
    )
    df["gmd_kg_dia"] = np.where(
        df["ganho_peso_kg"].notna() & (df["dias_confinado"] > 0),
        df["ganho_peso_kg"] / df["dias_confinado"], np.nan
    )

    # Arrobas
    df["@_arrobas"] = df["peso_carcaca_kg"] / 15.0

    # Tempo (sem locale)
    df["ano"] = dt_aba.dt.year
    df["mes"] = dt_aba.dt.month
    df["mes_nome"] = df["mes"].map(MAPA_MES)
    df["dia_mes"] = dt_aba.dt.day

    return df


@st.cache_data(ttl=60)
def fetch_lojas() -> list[str]:
    res = supabase.table("lojas").select("nome").order("nome").execute()
    return sorted([r["nome"] for r in (res.data or [])])


def upsert_abate(payload: dict):
    sel = supabase.table("abates").select("id") \
        .eq("codigo", payload["codigo"]).eq("data_abate", payload["data_abate"]).execute()
    if sel.data:
        supabase.table("abates").update(payload).eq("id", sel.data[0]["id"]).execute()
        log_usage("update", rows=1, notes={"codigo": payload["codigo"]})
    else:
        supabase.table("abates").insert(payload).execute()
        log_usage("insert", rows=1, notes={"codigo": payload["codigo"]})
    fetch_abates.clear()


def delete_abate(codigo: str, data_abate: date):
    supabase.table("abates").delete().eq("codigo", codigo).eq("data_abate", str(data_abate)).execute()
    log_usage("delete", rows=1, notes={"codigo": codigo})
    fetch_abates.clear()


# ---------------------------------------------------
# SIDEBAR: FILTROS (inclui dia do mês)
# ---------------------------------------------------
st.sidebar.divider()
st.sidebar.title("Filtro")

df_all_cache = fetch_abates()

anos = sorted(list({int(x) for x in df_all_cache["ano"].dropna().unique()})) or [datetime.now().year]
ano_sel = st.sidebar.selectbox("Ano do abate", anos, index=len(anos) - 1)

origem_sel = st.sidebar.multiselect("Origem", ORIGENS, default=ORIGENS)

lojas_existentes = sorted(df_all_cache.get("destino", pd.Series(dtype=str)).dropna().unique().tolist())
dest_sel = st.sidebar.multiselect("Destino (loja)", lojas_existentes, default=lojas_existentes)

meses_sel = st.sidebar.multiselect("Meses", MESES, default=MESES)

dias_disponiveis = sorted(df_all_cache.loc[df_all_cache["ano"] == ano_sel, "dia_mes"].dropna().unique().tolist())
dias_sel = st.sidebar.multiselect("Dias do mês", dias_disponiveis, default=dias_disponiveis)

# ---------------------------------------------------
# MONITOR DE USO (estimativa + opcional usage_events)
# ---------------------------------------------------
st.sidebar.divider()
st.sidebar.subheader("📈 Uso do plano (estimativa)")

if "logged_open" not in st.session_state:
    log_usage("open_app")
    st.session_state["logged_open"] = True

total_linhas = len(df_all_cache)
mb_est = (total_linhas * 0.5) / 1024  # ~0,5KB/linha
st.sidebar.write(f"Registros: **{total_linhas}**")
st.sidebar.write(f"Espaço estimado: **{mb_est:.2f} MB** / 500 MB")
st.sidebar.progress(min(1.0, mb_est / 500.0), text="Limite de 500 MB")

inicio_mes = datetime(datetime.now().year, datetime.now().month, 1)
try:
    usage = supabase.table("usage_events").select("id").gte("created_at", inicio_mes.isoformat()).execute()
    reqs_mes = len(usage.data or [])
    st.sidebar.write(f"Requisições (mês): **{reqs_mes}** / 50.000")
    st.sidebar.progress(min(1.0, reqs_mes / 50000.0), text="Limite de 50.000 req/mês")
except Exception:
    st.sidebar.write("Requisições (mês): **–** (sem logging configurado)")

# ---------------------------------------------------
# TABS
# ---------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "➕ Cadastro/Manutenção",
    "📊 Dashboard (Geral)",
    "📈 Tabelas & Indicadores",
    "🧠 Insights & Resumos"
])

# ===================================================
# TAB 1 — CADASTRO / MANUTENÇÃO
# ===================================================
with tab1:
    st.subheader("Cadastrar/Editar Abate")

    with st.form("form_cadastro", clear_on_submit=False):
        colA, colB, colC = st.columns(3)
        with colA:
            codigo = st.text_input("Código *", "")
        with colB:
            sexo = st.selectbox("Sexo *", SEXO, index=0)
        with colC:
            origem = st.selectbox("Origem *", ORIGENS, index=0)

        # Destino 1 (obrigatório) e Destino 2 (opcional)
        lojas = fetch_lojas()
        lojas_opt = lojas + ["+ Cadastrar nova loja…"]

        colD1, colD2 = st.columns(2)
        with colD1:
            destino1_sel = st.selectbox("Destino 1 (obrigatório) *", lojas_opt, index=0)
            if destino1_sel == "+ Cadastrar nova loja…":
                destino1 = st.text_input("Nova loja (Destino 1)", "")
            else:
                destino1 = destino1_sel

        with colD2:
            destino2_sel = st.selectbox("Destino 2 (opcional)", ["(sem 2ª loja)"] + lojas_opt, index=0)
            if destino2_sel == "+ Cadastrar nova loja…":
                destino2 = st.text_input("Nova loja (Destino 2)", "")
            elif destino2_sel == "(sem 2ª loja)":
                destino2 = None
            else:
                destino2 = destino2_sel

        colE, colF = st.columns(2)
        with colE:
            data_entrada = st.date_input("Data de Entrada no Confinamento (se houver)", value=None, format="DD/MM/YYYY")
        with colF:
            data_abate = st.date_input("Data do Abate *", value=date.today(), format="DD/MM/YYYY")

        # Pesos
        colG, colH, colI = st.columns([1, 1, 1])
        with colG:
            peso_entrada = st.number_input("Peso de Entrada (kg)", value=0.0, min_value=0.0, step=1.0)
        with colH:
            peso_abate = st.number_input("Peso de Abate (kg)", value=0.0, min_value=0.0, step=1.0)
        with colI:
            peso_carcaca = st.number_input("Peso de Carcaça (kg) (preferível)", value=0.0, min_value=0.0, step=1.0)

        rendimento_view = ""
        if peso_abate and peso_carcaca:
            rendimento_view = f"{(peso_carcaca / peso_abate) * 100:.2f}%"
        st.caption(f"Rendimento de carcaça (calculado pela tela): **{rendimento_view or '–'}**")

        submitted = st.form_submit_button("Salvar registro")
        if submitted:
            if not codigo or not data_abate:
                st.error("Preencha Código e Data do Abate.")
                st.stop()
            if not (destino1 or "").strip():
                st.error("Destino 1 é obrigatório.")
                st.stop()

            if destino1 and destino1 not in fetch_lojas():
                add_loja_if_new(destino1)
            if destino2 and destino2 not in fetch_lojas():
                add_loja_if_new(destino2)

            payload = {
                "codigo": codigo.strip(),
                "sexo": sexo,
                "origem": origem,
                "destino": destino1.strip() if destino1 else None,
                "destino2": destino2.strip() if destino2 else None,
                "data_entrada_confinamento": str(data_entrada) if data_entrada else None,
                "data_abate": str(data_abate),
                "peso_entrada_kg": float(peso_entrada) if peso_entrada else None,
                "peso_abate_kg": float(peso_abate) if peso_abate else None,
                "peso_carcaca_kg": float(peso_carcaca) if peso_carcaca else None,
                "rendimento_carcaca_pct": (
                    float(peso_carcaca) / float(peso_abate)
                    if (peso_abate and peso_carcaca) else None
                ),
            }
            upsert_abate(payload)
            st.success("Registro salvo.")

    st.divider()
    st.subheader("Registros (filtro rápido)")

    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        filtro_codigo = st.text_input("Filtrar por Código contém")
    with colf2:
        filtro_destino = st.text_input("Filtrar por Destino contém")
    with colf3:
        filtro_origem = st.multiselect("Filtrar por Origem", ORIGENS, default=ORIGENS)

    df_tab1 = fetch_abates().copy()
    if filtro_codigo:
        df_tab1 = df_tab1[df_tab1["codigo"].str.contains(filtro_codigo, case=False, na=False)]
    if filtro_destino:
        df_tab1 = df_tab1[
            df_tab1["destino"].str.contains(filtro_destino, case=False, na=False) |
            df_tab1.get("destino2", pd.Series(dtype=str)).fillna("").str.contains(filtro_destino, case=False, na=False)
        ]
    if filtro_origem:
        df_tab1 = df_tab1[df_tab1["origem"].isin(filtro_origem)]

    st.dataframe(
        df_tab1[[
            "codigo", "sexo", "origem", "destino", "destino2",
            "data_entrada_confinamento", "data_abate", "dias_confinado",
            "peso_entrada_kg", "peso_abate_kg", "peso_carcaca_kg",
            "ganho_peso_kg", "gmd_kg_dia", "@_arrobas",
            "rendimento_carcaca_pct"
        ]].fillna(""),
        use_container_width=True, hide_index=True
    )

    st.caption("Para excluir, informe Código + Data do Abate:")
    colx, coly, colz = st.columns(3)
    with colx:
        del_codigo = st.text_input("Código (excluir)")
    with coly:
        del_data = st.date_input("Data do Abate (excluir)", value=None, format="DD/MM/YYYY")
    with colz:
        if st.button("Excluir um registro"):
            if del_codigo and del_data:
                delete_abate(del_codigo.strip(), del_data)
                st.success("Registro excluído.")
            else:
                st.warning("Informe Código e Data do Abate para excluir.")

# ===================================================
# TAB 2 — DASHBOARD (Geral)
# ===================================================
with tab2:
    st.subheader("Visão geral (com filtros)")
    df_all = fetch_abates()

    mask_global = (
        (df_all["ano"] == ano_sel) &
        (df_all["origem"].isin(origem_sel)) &
        (df_all["mes_nome"].isin(meses_sel)) &
        (df_all["dia_mes"].isin(dias_sel)) &
        (df_all["destino"].isin(dest_sel if dest_sel else df_all["destino"].unique()))
    )
    df = df_all.loc[mask_global].copy()
    df_registros_filtrados = df.copy()

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Animais", len(df))
    with k2:
        st.metric("Peso de Carcaça (kg)", f"{df['peso_carcaca_kg'].sum():,.0f}".replace(",", "."))
    with k3:
        st.metric("GMD (kg/dia)", f"{df['gmd_kg_dia'].mean():.2f}" if not df.empty else "–")
    with k4:
        st.metric("Rendimento médio (%)", f"{(df['rendimento_carcaca_pct']*100).mean():.2f}" if not df.empty else "–")

    st.divider()

    # 6 gráficos (3 linhas x 2 colunas)
    g1 = df.groupby("mes_nome", as_index=False)["peso_carcaca_kg"].sum().pipe(ordenar_por_mes)
    g2 = df.groupby("origem", as_index=False)["peso_carcaca_kg"].sum().sort_values("peso_carcaca_kg", ascending=False)
    g3 = df.groupby("mes_nome", as_index=False)["rendimento_carcaca_pct"].mean().pipe(ordenar_por_mes)
    g3["rendimento_%"] = g3["rendimento_carcaca_pct"] * 100
    g4 = df.groupby("mes_nome", as_index=False)["gmd_kg_dia"].mean().pipe(ordenar_por_mes)
    g5 = df.groupby("mes_nome", as_index=False)["ganho_peso_kg"].mean().pipe(ordenar_por_mes)
    g6 = df.groupby("origem", as_index=False)["codigo"].count().rename(columns={"codigo": "qtd"}).sort_values("qtd", ascending=False)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(g1, x="mes_nome", y="peso_carcaca_kg",
                               title="Total de carcaça por mês (kg)"),
                        use_container_width=True)
    with c2:
        st.plotly_chart(px.bar(g2, x="origem", y="peso_carcaca_kg",
                               title="Total de carcaça por origem (kg)"),
                        use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.line(g3, x="mes_nome", y="rendimento_%",
                                markers=True, title="Rendimento médio por mês (%)"),
                        use_container_width=True)
    with c4:
        st.plotly_chart(px.line(g4, x="mes_nome", y="gmd_kg_dia",
                                markers=True, title="GMD médio por mês (kg/dia)"),
                        use_container_width=True)

    c5, c6 = st.columns(2)
    with c5:
        st.plotly_chart(px.bar(g5, x="mes_nome", y="ganho_peso_kg",
                               title="Ganho médio de peso por mês (kg)"),
                        use_container_width=True)
    with c6:
        st.plotly_chart(px.pie(g6, names="origem", values="qtd",
                               title="Distribuição de animais por origem"),
                        use_container_width=True)

    st.divider()
    csv_bytes = df_to_csv_bytes(df_registros_filtrados)
    st.download_button(
        "⬇ Baixar CSV (filtrado)", data=csv_bytes,
        file_name=f"abates_{ano_sel}_filtrado.csv", mime="text/csv",
        key="dl_csv_tab2"
    )
    log_usage("export_csv", rows=len(df_registros_filtrados))

# ===================================================
# TAB 3 — TABELAS & INDICADORES
# ===================================================
with tab3:
    st.subheader("Pivôs e Indicadores")
    df_all = fetch_abates()

    mask_global = (
        (df_all["ano"] == ano_sel) &
        (df_all["origem"].isin(origem_sel)) &
        (df_all["mes_nome"].isin(meses_sel)) &
        (df_all["dia_mes"].isin(dias_sel)) &
        (df_all["destino"].isin(dest_sel if dest_sel else df_all["destino"].unique()))
    )
    df = df_all.loc[mask_global].copy()
    df = ordenar_por_mes(df)

    with st.expander("Média de Rendimento de Carcaça (%) por origem x mês", expanded=True):
        pt_rend = (month_pivot(df, "rendimento_carcaca_pct", "mean") * 100).round(2)
        st.dataframe(pt_rend, use_container_width=True)

    with st.expander("Média de GMD (kg/dia) por origem x mês", expanded=False):
        pt_gmd = month_pivot(df, "gmd_kg_dia", "mean").round(2)
        st.dataframe(pt_gmd, use_container_width=True)

    with st.expander("Total de Carcaça (kg) por origem x mês", expanded=False):
        pt_carc = month_pivot(df, "peso_carcaca_kg", "sum").round(0)
        st.dataframe(pt_carc, use_container_width=True)

    st.divider()
    colx, _ = st.columns(2)
    with colx:
        excel_buf = make_excel_workbook(
            df,
            sheets={
                "Pivô_Rendimento(%)": pt_rend,
                "Pivô_GMD(kg_dia)": pt_gmd,
                "Pivô_Carcaça(kg)": pt_carc,
            }
        )
        st.download_button(
            "⬇ Baixar Excel (Registros + Pivôs)",
            data=excel_buf,
            file_name=f"abates_{ano_sel}_relatorio.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_xlsx_tab3"
        )
        log_usage("export_xlsx", rows=len(df))

# ===================================================
# TAB 4 — INSIGHTS & RESUMOS (dia, semana, mês)
# ===================================================
with tab4:
    st.subheader("Insights & Resumos")
    df_all = fetch_abates()

    mask_global = (
        (df_all["ano"] == ano_sel) &
        (df_all["origem"].isin(origem_sel)) &
        (df_all["mes_nome"].isin(meses_sel)) &
        (df_all["dia_mes"].isin(dias_sel)) &
        (df_all["destino"].isin(dest_sel if dest_sel else df_all["destino"].unique()))
    )
    dfi = df_all.loc[mask_global].copy()

    if dfi.empty:
        st.info("Sem dados para os filtros selecionados.")
        st.stop()

    dti = pd.to_datetime(dfi["data_abate"], errors="coerce")

    # Por dia
    grp_day = dfi.groupby(dti.dt.date).agg(
        animais=("codigo", "count"),
        carcaca_kg=("peso_carcaca_kg", "sum"),
        gmd_media=("gmd_kg_dia", "mean"),
        rendimento_pct=("rendimento_carcaca_pct", lambda s: (s.mean() * 100) if len(s) > 0 else np.nan)
    ).reset_index(names="data").sort_values("data")

    # Por semana (segunda como início)
    semana = dti.dt.to_period("W-MON").astype(str)
    grp_week = dfi.groupby(semana).agg(
        animais=("codigo", "count"),
        carcaca_kg=("peso_carcaca_kg", "sum"),
        gmd_media=("gmd_kg_dia", "mean"),
        rendimento_pct=("rendimento_carcaca_pct", lambda s: (s.mean() * 100) if len(s) > 0 else np.nan)
    ).reset_index(names="semana")

    # Por mês
    grp_month = dfi.groupby("mes_nome").agg(
        animais=("codigo", "count"),
        carcaca_kg=("peso_carcaca_kg", "sum"),
        gmd_media=("gmd_kg_dia", "mean"),
        rendimento_pct=("rendimento_carcaca_pct", lambda s: (s.mean() * 100) if len(s) > 0 else np.nan)
    ).reset_index()
    grp_month = ordenar_por_mes(grp_month)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Resumo por Dia**")
        st.dataframe(grp_day, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Resumo por Semana**")
        st.dataframe(grp_week, use_container_width=True, hide_index=True)

    st.markdown("**Resumo por Mês**")
    st.dataframe(grp_month, use_container_width=True, hide_index=True)

    st.divider()
    colD, colW, colM = st.columns(3)
    with colD:
        st.plotly_chart(
            px.bar(grp_day, x="data", y="carcaca_kg", title="Carcaça (kg) por Dia"),
            use_container_width=True
        )
    with colW:
        st.plotly_chart(
            px.bar(grp_week, x="semana", y="carcaca_kg", title="Carcaça (kg) por Semana"),
            use_container_width=True
        )
    with colM:
        st.plotly_chart(
            px.bar(grp_month, x="mes_nome", y="carcaca_kg", title="Carcaça (kg) por Mês"),
            use_container_width=True
        )
