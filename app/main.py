from fastapi import FastAPI, Depends
from app.settings import APP_NAME
from app.db import get_conn

app = FastAPI(title=APP_NAME)

"""
TO USE THE API, YOU NEED TO HAVE A POSTGRES DATABASE WITH THE FOLLOWING TABLES:
- customers
- accounts
- loans
- branches
- cards
- merchants

AND .env FILE WITH THE FOLLOWING VARIABLES:
- DB_HOST
- DB_PORT
- DB_NAME
- DB_USER
- DB_PASSWORD
"""


@app.get("/Liquidity")
def list_accounts(conn=Depends(get_conn)):
    """
    Loan-to-Deposit Ratio (LDR) - Liquidity KPI

    total amount of loans issued divided by the total amount of deposits (balance_usd in accounts).

    Data Science Use Case: If the LDR is too high, the bank might not have enough 
    liquidity to cover unforeseen fund requirements; if it's too low, the bank isn't earning as much as it could be.
    Contexto: Métricas de contexto para el análisis
    """
    cur = conn.cursor()

    try:
        cur.execute(
            """
            WITH total_portfolio_loans AS (
                -- Aggregates the total outstanding capital disbursed
                SELECT COALESCE(SUM(loan_amount), 0) AS total_loans 
                FROM loans
            ),
            total_portfolio_deposits AS (
                -- Aggregates the total capital held in customer accounts
                SELECT COALESCE(SUM(balance_usd), 0) AS total_deposits 
                FROM accounts
            )
            SELECT 
                l.total_loans,
                d.total_deposits,
                -- Calculates the ratio, handling division by zero
                (l.total_loans / NULLIF(d.total_deposits, 0)) AS loan_to_deposit_ratio,
                -- Standard financial format (Percentage)
                (l.total_loans / NULLIF(d.total_deposits, 0)) * 100 AS ldr_percentage
            FROM 
                total_portfolio_loans l
            CROSS JOIN 
                total_portfolio_deposits d;
            """
        )
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()

@app.get("/ClientsCount")
def list_accounts(conn=Depends(get_conn)):
    """
    KPI 1: Total de clientes
    Real: Suma de los montos de los préstamos activos
    Contexto: Métricas de contexto para el análisis
    Contrastar: Promedio Simple (Para contrastar la distorsión)
    """
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT COUNT(*) customer_id FROM customers;
            """
        )
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()

@app.get("/TotalActiveLoans")
def list_accounts(conn=Depends(get_conn)):
    """
    KPI 3: Total de préstamos activos
    Real: Suma de los montos de los préstamos activos
    Contexto: Métricas de contexto para el análisis
    Contrastar: Promedio Simple (Para contrastar la distorsión)
    """
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT 
                SUM(loan_amount) AS total_active_loan_exposure
            FROM 
                loans
            /* WHERE status = 'active'
            -- ATENCIÓN: La columna 'status' no existe en el esquema actual. 
            -- Descomentar esta línea causará un SQL Error [42703].
            */
            ;
            """
        )
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()


@app.get("/YieldProfile")
def list_accounts(conn=Depends(get_conn)):
    """
    KPI 2: Yield Profile
    Real: Promedio Ponderado (El KPI real)
    Contexto: Métricas de contexto para el análisis
    Contrastar: Promedio Simple (Para contrastar la distorsión)
    """
    cur = conn.cursor()

    try:
        cur.execute(
            
        """
        SELECT 
            -- 1. Promedio Ponderado (El KPI real)
            SUM(interest_rate * loan_amount) / SUM(loan_amount) AS weighted_yield_profile,
            
            -- 2. Métricas de contexto para el análisis
            SUM(loan_amount) AS total_portfolio_size,
            
            -- 3. Promedio Simple (Para contrastar la distorsión)
            AVG(interest_rate) AS simple_average_yield
        FROM 
            loans
        WHERE 
            loan_amount > 0; -- Buena práctica de Data Quality para evitar división por cero
"""
        )
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()

@app.get("/CreditScoreBins")
def list_accounts(conn=Depends(get_conn)):
    """
    What it measures: The total monetary exposure 
    (loan_amount) grouped by the customer's credit score buckets.

    Use Case: This teaches you Discretization/Binning, a core Feature Engineering 
    technique where continuous variables (like a credit score from 300 to 850)
     are transformed into categorical features (like "Excellent", "Good", "Poor") to find non-linear patterns in machine learning models.   
    """
    cur = conn.cursor()

    try:
        cur.execute(

            """
            SELECT 
                -- 1. Feature Engineering: Discretization (Binning) of the continuous credit score
                CASE 
                    WHEN c.credit_score >= 800 THEN '1 - Excellent (800+)'
                    WHEN c.credit_score >= 740 THEN '2 - Very Good (740-799)'
                    WHEN c.credit_score >= 670 THEN '3 - Good (670-739)'
                    WHEN c.credit_score >= 580 THEN '4 - Fair (580-669)'
                    ELSE '5 - Poor (<580)'
                END AS risk_tier,
                
                -- 2. Volume and Exposure Metrics
                COUNT(l.loan_id) AS total_loans_issued,
                SUM(l.loan_amount) AS total_monetary_exposure,
                
                -- 3. True Yield Profile (Weighted Average to avoid distortion)
                SUM(l.interest_rate * l.loan_amount) / NULLIF(SUM(l.loan_amount), 0) AS weighted_yield
            FROM 
                customers c
            JOIN 
                loans l ON c.customer_id = l.customer_id
            GROUP BY 
                risk_tier
            ORDER BY 
                risk_tier ASC;
        """
        )
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()

