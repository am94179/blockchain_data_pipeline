"""Idempotently create Gold-layer transaction charts and their dashboard."""

import json

DATABASE_NAME = "Trino Iceberg"
SCHEMA = "gold"
DASHBOARD_TITLE = "Bitcoin Transaction Activity"
DASHBOARD_SLUG = "bitcoin-transaction-activity"


def temporal_filter(column):
    return {"clause": "WHERE", "subject": column, "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}


def metric(column):
    return {"expressionType": "SIMPLE", "column": {"column_name": column}, "aggregate": "SUM", "sqlExpression": None, "label": f"SUM({column})"}


def params(dataset_id, viz_type, **values):
    return {"datasource": f"{dataset_id}__table", "viz_type": viz_type, **values}


def layout(charts):
    result = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]}, "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []}, "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}}}
    for row_name, row_charts in (("kpis", charts[:4]), ("analysis", charts[4:6]), ("detail", charts[6:])):
        row_id = f"ROW-{row_name}"
        result["GRID_ID"]["children"].append(row_id)
        result[row_id] = {"id": row_id, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"], "children": [], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for chart in row_charts:
            chart_id = f"CHART-{chart.id}"
            result[row_id]["children"].append(chart_id)
            result[chart_id] = {"id": chart_id, "type": "CHART", "parents": ["ROOT_ID", "GRID_ID", row_id], "children": [], "meta": {"chartId": chart.id, "sliceName": chart.slice_name, "height": 42 if row_name == "kpis" else 72, "width": 12 // len(row_charts)}}
    return result


def main():
    from superset.app import create_app
    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable, TableColumn
        from superset.extensions import db
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
        if database is None:
            raise RuntimeError(f"Missing Superset database connection: {DATABASE_NAME}")
        transactions = db.session.query(SqlaTable).filter_by(database_id=database.id, schema=SCHEMA, table_name="transactions").one()
        blocks = db.session.query(SqlaTable).filter_by(database_id=database.id, schema=SCHEMA, table_name="blocks").one()
        try:
            existing = {column.column_name: column for column in transactions.columns}
            for name, expression, label in (
                ("transaction_shape", "CASE WHEN input_count = 1 AND output_count = 1 THEN '1 input / 1 output' WHEN input_count = 1 THEN '1 input / many outputs' WHEN output_count = 1 THEN 'many inputs / 1 output' ELSE 'many inputs / many outputs' END", "Transaction Shape"),
                ("short_tx_hash", "substr(tx_hash, 1, 12)", "Transaction Hash (short)"),
            ):
                column = existing.get(name)
                if column is None:
                    column = TableColumn(column_name=name, table=transactions)
                    db.session.add(column)
                column.expression, column.type, column.verbose_name = expression, "VARCHAR", label
                column.groupby = column.filterable = column.is_active = True
            db.session.flush()

            specs = [
                ("KPI · Total Transactions", transactions, "big_number_total", params(transactions.id, "big_number_total", metric="transaction_count", adhoc_filters=[temporal_filter("transaction_date")], y_axis_format="SMART_NUMBER")),
                ("KPI · Total Fees (sats)", transactions, "big_number_total", params(transactions.id, "big_number_total", metric="total_fee_sats", adhoc_filters=[temporal_filter("transaction_date")], y_axis_format="SMART_NUMBER")),
                ("KPI · Average Fee Rate (sats/byte)", transactions, "big_number_total", params(transactions.id, "big_number_total", metric="average_fee_rate_sats_per_byte", adhoc_filters=[temporal_filter("transaction_date")], y_axis_format="SMART_NUMBER")),
                ("KPI · Blocks Processed", blocks, "big_number_total", params(blocks.id, "big_number_total", metric="block_count", adhoc_filters=[temporal_filter("block_date")], y_axis_format="SMART_NUMBER")),
                ("Fee-Rate Distribution", transactions, "histogram_v2", params(transactions.id, "histogram_v2", column="fee_rate_sats_per_byte", groupby=[], adhoc_filters=[temporal_filter("transaction_date")], row_limit=25000, bins=20, normalize=False, show_legend=False)),
                ("Fee versus Transaction Size", transactions, "bubble_v2", params(transactions.id, "bubble_v2", entity="tx_hash", x=metric("size_bytes"), y=metric("fee_sats"), size="count", adhoc_filters=[temporal_filter("transaction_date")], row_limit=25000, max_bubble_size="14", opacity=0.55, xAxisFormat="SMART_NUMBER", y_axis_format="SMART_NUMBER", show_legend=False)),
                ("Transaction Input/Output Composition", transactions, "echarts_timeseries_bar", params(transactions.id, "echarts_timeseries_bar", x_axis="transaction_shape", metrics=["transaction_count"], adhoc_filters=[temporal_filter("transaction_date")], row_limit=100, orientation="vertical", show_legend=False, y_axis_format="SMART_NUMBER")),
                ("Top-Fee Transactions", transactions, "table", params(transactions.id, "table", query_mode="raw", groupby=[], all_columns=["short_tx_hash", "fee_sats", "size_bytes", "fee_rate_sats_per_byte", "input_count", "output_count"], percent_metrics=[], adhoc_filters=[temporal_filter("transaction_date")], order_by_cols=['["fee_sats", false]'], row_limit=20, server_page_length=20, order_desc=True)),
            ]
            saved = []
            for name, dataset, viz_type, chart_params in specs:
                chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
                status = "updated"
                if chart is None:
                    chart, status = Slice(slice_name=name), "created"
                    db.session.add(chart)
                chart.datasource_id, chart.datasource_type, chart.datasource_name = dataset.id, "table", dataset.full_name
                chart.viz_type, chart.params = viz_type, json.dumps(chart_params)
                chart.description = f"Bootstrap-managed chart based on {SCHEMA}.{dataset.table_name}."
                db.session.flush()
                saved.append(chart)
                print(f"{status} chart: {name}")
            dashboard = db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
            status = "updated"
            if dashboard is None:
                dashboard, status = Dashboard(dashboard_title=DASHBOARD_TITLE, slug=DASHBOARD_SLUG), "created"
                db.session.add(dashboard)
            dashboard.dashboard_title, dashboard.description, dashboard.published = DASHBOARD_TITLE, "Gold-layer transaction analytics queried through Trino.", True
            dashboard.slices, dashboard.position_json = saved, json.dumps(layout(saved))
            db.session.commit()
            print(f"{status} dashboard: {DASHBOARD_TITLE}")
        except Exception:
            db.session.rollback()
            raise


if __name__ == "__main__":
    main()
