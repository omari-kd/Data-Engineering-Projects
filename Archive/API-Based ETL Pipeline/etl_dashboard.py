from flask import Flask, render_template_string
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px

# Airflow metadata DB (adjust this to match your setup)
# AIRFLOW_DB = "sqlite:////home/yourusername/airflow/airflow.db"
# If using PostgreSQL instead, use:
AIRFLOW_DB = "postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow"

airflow_engine = create_engine(AIRFLOW_DB)


# Database connection 
DB_NAME = 'etl_demo'
USER = 'postgres'
PASSWORD = 'ben/junior'
HOST = 'localhost'
PORT = '5432'

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

# Flask app
app = Flask(__name__)


@app.route('/')
def dashboard():
    # Query data from the products table
    query = "SELECT * FROM products"
    df = pd.read_sql(query, engine)

    # Simple metrics
    total_products = len(df)
    avg_price = round(df['unit_price'].mean(), 2)
    avg_rating = round(df['rating_score'].mean(), 2)

    # Plotly chart: Category vs Average Price
    fig = px.bar(
        df.groupby('category_name')['unit_price'].mean().reset_index(),
        x='category_name',
        y='unit_price',
        title='Average Price by Category',
        text_auto=True
    )
    chart_html = fig.to_html(full_html=False)

    def get_dag_status():
        query = """
        SELECT
            dag_id,
            logical_date,
            state,
            run_id,
            end_date,
            start_date
        FROM dag_run
        ORDER BY logical_date DESC
        LIMIT 5;
        """
        df = pd.read_sql(query, airflow_engine)
        if not df.empty:
            df['logical)date'] = pd.to_datetime(df['logical_date']).dt.strftime("%Y-%m-%d %H:%M:%S")
            df['duration'] = (pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.seconds
        return df
    

    # --- NEW: Airflow DAG monitoring section ---
    dag_df = get_dag_status()

    dag_table_html = dag_df.to_html(classes="table table-striped", index=False) if not dag_df.empty else "No DAG runs found."

    # HTML Template
    html = """
    <html>
        <head>
            <title>ETL Monitoring Dashboard</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 40px;
                    background-color: #f4f4f9;
                }
                h1 {
                    color: #2c3e50;
                }
                .metrics {
                    display: flex;
                    justify-content: space-between;
                    margin-bottom: 30px;
                }
                .card {
                    background-color: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                    width: 30%;
                    text-align: center;
                }
                iframe {
                    width: 100%;
                    height: 500px;
                    border: none;
                }
            </style>
        </head>
        <body>
        <h1>ETL Monitoring Dashboard</h1>
        <div class="metrics">
                <div class="card">
                    <h3>Total Products</h3>
                    <p><b>{{ total_products }}</b></p>
                </div>
                <div class="card">
                    <h3>Average Price ($)</h3>
                    <p><b>{{ avg_price }}</b></p>
                </div>
                <div class="card">
                    <h3>Average Rating</h3>
                    <p><b>{{ avg_rating }}</b></p>
                </div>
            </div>
            <h2>Average Price by Category</h2>
            {{ chart_html|safe }}


        <h2>Recent Airflow DAG Runs</h2>
        {{dag_table_html}}
        </body>
    </html>
    """
    return render_template_string(
         html,
        total_products=total_products,
        avg_price=avg_price,
        avg_rating=avg_rating,
        chart_html=chart_html,
        dag_table_html=dag_table_html
    )

if __name__ == '__main__':
    app.run(debug=True)