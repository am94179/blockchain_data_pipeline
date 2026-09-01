"""Idempotently register the roadmap's physical Gold datasets in Superset."""

DATABASE_NAME = "Trino Iceberg"
SCHEMA = "gold"
DATASETS = {
    "transactions": {
        "temporal": "transaction_date",
        "description": "Gold transaction facts with input/output reconciliation metrics.",
        "metrics": {
            "transaction_count": ("COUNT(*)", "Transaction Count"),
            "total_fee_sats": ("SUM(fee_sats)", "Total Fee (sats)"),
            "average_fee_sats": ("AVG(fee_sats)", "Average Fee (sats)"),
            "average_fee_rate_sats_per_byte": ("AVG(fee_rate_sats_per_byte)", "Average Fee Rate (sats/byte)"),
            "total_output_sats": ("SUM(total_output_sats)", "Total Output Value (sats)"),
        },
    },
    "outputs": {
        "temporal": "output_date",
        "description": "Gold outputs with canonical spent-state reconciliation and lifecycle fields.",
        "metrics": {
            "output_count": ("COUNT(*)", "Output Count"),
            "total_output_sats": ("SUM(value_sats)", "Total Output Value (sats)"),
            "spend_discrepancy_count": ("COUNT_IF(spend_discrepancy)", "Spend Discrepancy Count"),
            "canonical_spent_count": ("COUNT_IF(canonical_spent)", "Canonically Spent Count"),
        },
    },
    "inputs": {
        "temporal": "spend_date",
        "description": "Gold inputs enriched with UTXO reference-resolution fields.",
        "metrics": {
            "input_count": ("COUNT(*)", "Input Count"),
            "total_input_sats": ("SUM(value_sats)", "Total Input Value (sats)"),
            "double_spend_count": ("COUNT_IF(double_spend_detected)", "Double-Spend Count"),
        },
    },
    "blocks": {
        "temporal": "block_date",
        "description": "Gold block facts enriched with observed transaction and fee aggregates.",
        "metrics": {
            "block_count": ("COUNT(*)", "Block Count"),
            "total_fee_sats": ("SUM(total_fee_sats)", "Total Fee (sats)"),
            "observed_transaction_count": ("SUM(observed_transaction_count)", "Observed Transaction Count"),
            "average_difficulty": ("AVG(difficulty)", "Average Difficulty"),
        },
    },
}


def main():
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlMetric, SqlaTable
        from superset.extensions import db
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
        if database is None:
            raise RuntimeError(f"Missing Superset database connection: {DATABASE_NAME}")

        try:
            for table_name, config in DATASETS.items():
                dataset = db.session.query(SqlaTable).filter_by(
                    database_id=database.id, schema=SCHEMA, table_name=table_name
                ).one_or_none()
                status = "updated"
                if dataset is None:
                    dataset = SqlaTable(database=database, schema=SCHEMA, table_name=table_name)
                    db.session.add(dataset)
                    status = "created"
                dataset.description = config["description"]
                dataset.main_dttm_col = config["temporal"]
                dataset.fetch_metadata()
                dataset.main_dttm_col = config["temporal"]
                existing_metrics = {metric.metric_name: metric for metric in dataset.metrics}
                for name, (expression, verbose_name) in config["metrics"].items():
                    metric = existing_metrics.get(name)
                    if metric is None:
                        metric = SqlMetric(metric_name=name, table=dataset)
                        db.session.add(metric)
                    metric.expression = expression
                    metric.verbose_name = verbose_name
                db.session.flush()
                print(f"{status} dataset: {SCHEMA}.{table_name}")
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise


if __name__ == "__main__":
    main()
