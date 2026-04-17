# VisualizationAPI
Josue David Chan Negroe
Nahum Francisco Massa Mandujano
Elio Eduardo Ucan Zapata

Loads CSV data files into a PostgreSQL database for visualization and analysis.

## Requirements

- Python 3.8+
- PostgreSQL

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and fill in your database credentials:

```bash
cp .env.example .env
```

| Variable      | Description              | Default     |
|---------------|--------------------------|-------------|
| `DB_HOST`     | PostgreSQL host          | `localhost` |
| `DB_PORT`     | PostgreSQL port          | `5432`      |
| `DB_NAME`     | Database name            | —           |
| `DB_USER`     | Database user            | —           |
| `DB_PASSWORD` | Database password        | _(empty)_   |

## Data

CSV files in `data/` are loaded automatically. Each file becomes a table named after the file stem (e.g. `customers.csv` → `customers`). All columns are created as `TEXT`.

Current datasets:

- `accounts.csv`
- `branches.csv`
- `cards.csv`
- `customers.csv`
- `loans.csv`
- `merchants.csv`

## Usage

```bash
python scripts/load_csv_to_db.py
```

The script will:
1. Connect to the database using the `.env` credentials.
2. For each CSV in `data/`, create the table if it does not exist.
3. Insert all rows into the corresponding table.
