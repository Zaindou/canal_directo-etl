from db_connection import DBConnection
from bq_connection import BQConnection

db = DBConnection()
bq = BQConnection()


def get_client_qnt():
    query = """
    SELECT numero_identificacion, MAX(id_cliente_sf) as id_cliente_sf, nombre_hc,
    etapa_maxima, operado_por, puntaje_crediticio, objetivo_financiero, 
    porcentaje_avance_objetivo, calificacion_riesgo, ingresos_mensuales, 
    cantidad_productos, productos_mora, saldo_total_general, 
    saldo_deudas_general, MAX(fecha_creacion) as fecha_creacion
    FROM canal_directo_financialdiagnostic as fd
    JOIN canal_directo_diagnosticresult as dr ON fd.numero_identificacion = dr.diagnostic_id 
    JOIN canal_directo_products as p ON fd.numero_identificacion = p.product_id
    GROUP BY numero_identificacion, nombre_hc, etapa_maxima, operado_por,
    puntaje_crediticio, objetivo_financiero, porcentaje_avance_objetivo, 
    calificacion_riesgo, ingresos_mensuales, cantidad_productos, productos_mora, 
    saldo_total_general, saldo_deudas_general
    """
    return db.fetch(query)


def get_products_other_entities():
    query = """
    SELECT numero_identificacion, id_cliente_sf, nombre_hc, 
    entidad, tipo_cuenta, id_cuenta, estado, antiguedad, dias_en_mora, saldo_total, cuota, participacion, participacion_mora, fecha_creacion
    FROM canal_directo_financialdiagnostic as fd
    JOIN canal_directo_diagnosticresult as dr ON fd.numero_identificacion = dr.diagnostic_id 
    JOIN canal_directo_products as p ON fd.numero_identificacion = p.product_id
    WHERE p.es_producto_qnt = False
    AND WHERE DATE(fecha_creacion) = DATE(NOW())
    """
    return db.fetch(query)


def get_qnt_products():
    query = """
    SELECT numero_identificacion, id_cliente_sf, nombre_hc, operado_por, objetivo_financiero,
    entidad, tipo_cuenta, id_cuenta, estado, antiguedad, dias_en_mora, saldo_total, cuota, participacion_mora, fecha_creacion
    FROM canal_directo_financialdiagnostic as fd
    JOIN canal_directo_diagnosticresult as dr ON fd.numero_identificacion = dr.diagnostic_id 
    JOIN canal_directo_products as p ON fd.numero_identificacion = p.product_id
    WHERE p.es_producto_qnt = True
    """
    return db.fetch(query)


def get_contact_other_entities():
    query = """
    SELECT nombre_entidad, numero_contacto, correo_contacto FROM canal_directo_contactentity
    """
    return db.fetch(query)
