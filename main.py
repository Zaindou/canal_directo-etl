from utils import (
    bq,
    get_client_qnt,
    get_products_other_entities,
    get_contact_other_entities,
    get_qnt_products,
)
from canaldirecto_api import CanalDirectoConection
import pandas as pd


def load_client_data_to_bq():
    try:
        db_data = get_client_qnt()

        df = pd.DataFrame(
            db_data,
            columns=[
                "numero_identificacion",
                "id_cliente_sf",
                "nombre_cliente",
                "etapa_diagnostico",
                "operador",
                "puntaje_crediticio",
                "objetivo_financiero",
                "porcentaje_avance_objetivo",
                "calificacion_riesgo",
                "ingresos_mensuales",
                "productos_vigentes",
                "productos_mora",
                "saldo_total_general",
                "saldo_mora_general",
                "fecha_diagnostico",
            ],
        )
        table_id = "Digital1.Canal_directo_clientes"
        bq.load_data(df, table_id)

        return True
    except Exception as e:
        print(e, "Error al cargar los datos a BigQuery")
        return False


def load_other_products_entities_to_bq():
    try:
        products_data = get_products_other_entities()
        contact_data = get_contact_other_entities()

        products_df = pd.DataFrame(
            products_data,
            columns=[
                "numero_identificacion",
                "id_cliente_sf",
                "nombre_cliente",
                "entidad",
                "tipo_cuenta",
                "id_cuenta",
                "estado",
                "antiguedad",
                "dias_en_mora",
                "saldo_total",
                "cuota",
                "participacion",
                "participacion_mora",
                "fecha_diagnostico",
            ],
        )

        contact_df = pd.DataFrame(
            contact_data,
            columns=["entidad", "numero_contacto", "correo_contacto"],
        )

        merged_df = pd.merge(products_df, contact_df, on="entidad", how="left")

        merged_df.fillna("Sin informacion.", inplace=True)
        table_id = "Digital1.Canal_directo_otras_entidades"
        bq.load_data(merged_df, table_id)

        return True
    except Exception as e:
        print(e, "Error al cargar los datos a BigQuery")
        return False


def load_qnt_products():
    try:
        products_data = get_qnt_products()

        products_df = pd.DataFrame(
            products_data,
            columns=[
                "numero_identificacion",
                "id_cliente_sf",
                "nombre_cliente",
                "objetivo_financiero",
                "entidad",
                "tipo_cuenta",
                "id_cuenta",
                "estado",
                "antiguedad",
                "dias_en_mora",
                "saldo_total",
                "cuota_promedio",
                "participacion_mora",
                "fecha_diagnostico",
            ],
        )

        api_data = [
            CanalDirectoConection(id).get()
            for id in products_df["numero_identificacion"].unique()
        ]

        api_df_list = []
        for data_dict in api_data:
            for item in data_dict.get("wazeQnt", []):
                for term in item["scores_by_term"].keys():
                    term_data = item["scores_by_term"].get(term, {})
                    offers_data = (
                        item["ofertas"].get(term, {}) if item["ofertas"] else {}
                    )
                    selected_data = {
                        "producto": item.get("producto", None),
                        "monto_oferta": offers_data.get("monto_final_oferta", None),
                        "saldo_capital": offers_data.get("saldo_capital", None),
                        "plazo": term,
                        "cuota": offers_data.get("cuota", None),
                        "puntaje_por_cuota": term_data.get("puntaje_por_cuota", None),
                        "nuevo_puntaje": term_data.get("nuevo_puntaje", None),
                        "porcentaje_hacia_objetivo": term_data.get(
                            "hacia_objetivo", None
                        ),
                        "tiempo_para_objetivo_meses": term_data.get(
                            "tiempo_meses", None
                        ),
                    }
                    api_df_list.append(selected_data)

        api_df = pd.DataFrame(api_df_list)

        final_df = pd.merge(
            products_df, api_df, left_on="id_cuenta", right_on="producto", how="left"
        )

        table_id = "Digital1.Canal_directo_productos_qnt"
        bq.load_data(final_df, table_id)

        return True

    except Exception as e:
        print(e, "Error al cargar los datos a BigQuery")
        return False


print(load_qnt_products())
