import psycopg2
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('.env').resolve(), override=True)

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'odoo'),
    'user':     os.getenv('DB_USER', 'odoo'),
    'password': os.getenv('DB_PASSWORD', ''),
}


def run_query():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = """
    WITH RECURSIVE location_tree AS (
        SELECT id
        FROM stock_location
        WHERE id = 1

        UNION ALL

        SELECT sl.id
        FROM stock_location sl
        JOIN location_tree lt ON sl.location_id = lt.id
    ),

    sales_data AS (
        SELECT
            sml.product_id,
            SUM(sml.qty_done) AS total_sold
        FROM stock_move_line sml
        JOIN stock_picking sp ON sp.id = sml.picking_id
        WHERE
            sml.state = 'done'
            AND sml.date BETWEEN '2025-01-01' AND '2025-01-07'
        GROUP BY sml.product_id
    ),

    stock_data AS (
        SELECT 
            sq.product_id,
            SUM(sq.quantity - sq.reserved_quantity) AS available_qty
        FROM stock_quant sq
        GROUP BY sq.product_id
    ),

    opening_stock AS (
        SELECT 
            product_id,
            SUM(qty_done) AS opening_qty
        FROM stock_move_line
        WHERE date < '2025-01-01'
        GROUP BY product_id
    )

    SELECT
        COALESCE(sd.product_id, st.product_id) AS product_id,
        COALESCE(st.available_qty, 0) AS current_stock_qty,
        COALESCE(sd.total_sold, 0) AS total_sold,

        CASE 
            WHEN COALESCE(sd.total_sold, 0) > 0
            THEN sd.total_sold / 7.0
            ELSE 0
        END AS adu_value,

        CASE 
            WHEN COALESCE(sd.total_sold, 0) > 0
            THEN COALESCE(st.available_qty, 0) 
                 / NULLIF(sd.total_sold / 7.0, 0)
            ELSE NULL
        END AS dii_value

    FROM sales_data sd
    FULL OUTER JOIN stock_data st
        ON sd.product_id = st.product_id;
    """

    cur.execute(query)
    rows = cur.fetchall()

    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    cur.close()
    conn.close()

    return df


if __name__ == "__main__":
    df = run_query()
    print(df.head())