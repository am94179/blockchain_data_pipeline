"""
PySpark practice tests.

Run with:
    pytest -v test_pyspark.py

Requirements:
    pip install pyspark pytest

No Spark cluster or Docker container is required.
Spark runs locally via .master("local[2]").
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Spark fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    """Create one local Spark session for the entire test session."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-practice")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------


@pytest.fixture
def employees(spark):
    """A small DataFrame we'll use throughout the exercises."""
    data = [
        (1, "Alice", "Engineering", 30, 100000),
        (2, "Bob", "Engineering", 25, 85000),
        (3, "Charlie", "Sales", 35, 90000),
        (4, "Diana", "Sales", 28, 75000),
        (5, "Eve", "Engineering", 40, 120000),
        (6, "Frank", "HR", 32, 70000),
        (7, "Grace", "HR", 26, 65000),
    ]

    return spark.createDataFrame(
        data,
        ["id", "name", "department", "age", "salary"],
    )


# ===========================================================================
# 1. SELECT
# ===========================================================================


def test_select(employees):
    result = employees.select("name", "salary")

    assert result.columns == ["name", "salary"]

    rows = result.collect()

    assert rows[0]["name"] == "Alice"
    assert rows[0]["salary"] == 100000


# ===========================================================================
# 2. FILTER / WHERE
# ===========================================================================


def test_filter(employees):
    result = employees.filter(F.col("salary") >= 90000)

    names = {row["name"] for row in result.collect()}

    assert names == {"Alice", "Charlie", "Eve"}


def test_where(employees):
    result = employees.where(F.col("age") < 30)

    names = {row["name"] for row in result.collect()}

    assert names == {"Bob", "Diana", "Grace"}


# ===========================================================================
# 3. WITHCOLUMN
# ===========================================================================


def test_with_column(employees):
    result = employees.withColumn(
        "salary_k",
        F.col("salary") / 1000,
    )

    alice = result.filter(F.col("name") == "Alice").first()

    assert alice["salary_k"] == 100


# ===========================================================================
# 4. WHEN / OTHERWISE
# ===========================================================================


def test_when_otherwise(employees):
    result = employees.withColumn(
        "seniority",
        F.when(F.col("age") >= 35, "Senior")
        .when(F.col("age") >= 30, "Mid")
        .otherwise("Junior"),
    )

    alice = result.filter(F.col("name") == "Alice").first()
    eve = result.filter(F.col("name") == "Eve").first()
    bob = result.filter(F.col("name") == "Bob").first()

    assert alice["seniority"] == "Mid"
    assert eve["seniority"] == "Senior"
    assert bob["seniority"] == "Junior"


# ===========================================================================
# 5. RENAME / DROP
# ===========================================================================


def test_rename_and_drop(employees):
    result = employees.withColumnRenamed("salary", "annual_salary").drop("age")

    assert "annual_salary" in result.columns
    assert "salary" not in result.columns
    assert "age" not in result.columns


# ===========================================================================
# 6. SORT / ORDER BY
# ===========================================================================


def test_order_by(employees):
    result = employees.orderBy(F.col("salary").desc())

    names = [row["name"] for row in result.select("name").collect()]

    assert names[0] == "Eve"
    assert names[-1] == "Grace"


# ===========================================================================
# 7. DISTINCT
# ===========================================================================


def test_distinct(employees):
    result = employees.select("department").distinct()

    departments = {row["department"] for row in result.collect()}

    assert departments == {"Engineering", "Sales", "HR"}


# ===========================================================================
# 8. GROUP BY / AGGREGATION
# ===========================================================================


def test_group_by(employees):
    result = employees.groupBy("department").agg(
        F.count("*").alias("employee_count"),
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )

    engineering = result.filter(F.col("department") == "Engineering").first()

    assert engineering["employee_count"] == 3
    assert engineering["max_salary"] == 120000


# ===========================================================================
# 9. MULTIPLE AGGREGATIONS
# ===========================================================================


def test_multiple_aggregations(employees):
    result = (
        employees.groupBy("department")
        .agg(
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary"),
            F.sum("salary").alias("total_salary"),
        )
        .orderBy("department")
    )

    engineering = result.filter(F.col("department") == "Engineering").first()

    assert engineering["min_salary"] == 85000
    assert engineering["max_salary"] == 120000
    assert engineering["total_salary"] == 305000


# ===========================================================================
# 10. STRING FUNCTIONS
# ===========================================================================


def test_string_functions(employees):
    result = employees.withColumn(
        "upper_name",
        F.upper(F.col("name")),
    )

    alice = result.filter(F.col("id") == 1).first()

    assert alice["upper_name"] == "ALICE"


# ===========================================================================
# 11. CONCAT
# ===========================================================================


def test_concat(employees):
    result = employees.withColumn(
        "label",
        F.concat(
            F.col("name"),
            F.lit(" - "),
            F.col("department"),
        ),
    )

    alice = result.filter(F.col("id") == 1).first()

    assert alice["label"] == "Alice - Engineering"


# ===========================================================================
# 12. SQL EXPRESSIONS
# ===========================================================================


def test_expr(employees):
    result = employees.withColumn(
        "monthly_salary",
        F.expr("salary / 12"),
    )

    alice = result.filter(F.col("name") == "Alice").first()

    assert round(alice["monthly_salary"], 2) == 8333.33


# ===========================================================================
# 13. JOIN
# ===========================================================================


def test_join(spark, employees):
    departments = spark.createDataFrame(
        [
            ("Engineering", "New York"),
            ("Sales", "Chicago"),
            ("HR", "Boston"),
        ],
        ["department", "office"],
    )

    result = employees.join(
        departments,
        on="department",
        how="inner",
    )

    alice = result.filter(F.col("name") == "Alice").first()

    assert alice["office"] == "New York"


# ===========================================================================
# 14. LEFT JOIN
# ===========================================================================


def test_left_join(spark, employees):
    managers = spark.createDataFrame(
        [
            ("Engineering", "Sarah"),
            ("Sales", "Mike"),
        ],
        ["department", "manager"],
    )

    result = employees.join(
        managers,
        on="department",
        how="left",
    )

    hr_employee = result.filter(F.col("department") == "HR").first()

    assert hr_employee["manager"] is None


# ===========================================================================
# 15. NULL HANDLING
# ===========================================================================


def test_null_handling(spark):
    df = spark.createDataFrame(
        [
            ("Alice", 100),
            ("Bob", None),
            ("Charlie", 50),
        ],
        ["name", "bonus"],
    )

    result = df.withColumn(
        "bonus_filled",
        F.coalesce(F.col("bonus"), F.lit(0)),
    )

    bob = result.filter(F.col("name") == "Bob").first()

    assert bob["bonus"] is None
    assert bob["bonus_filled"] == 0


# ===========================================================================
# 16. DATE FUNCTIONS
# ===========================================================================


def test_date_functions(spark):
    df = spark.createDataFrame(
        [
            ("Alice", "2026-01-15"),
            ("Bob", "2026-06-20"),
        ],
        ["name", "hire_date"],
    )

    result = (
        df.withColumn(
            "hire_date",
            F.to_date("hire_date"),
        )
        .withColumn(
            "year",
            F.year("hire_date"),
        )
        .withColumn(
            "month",
            F.month("hire_date"),
        )
    )

    alice = result.filter(F.col("name") == "Alice").first()

    assert alice["year"] == 2026
    assert alice["month"] == 1


# ===========================================================================
# 17. UNION
# ===========================================================================


def test_union(spark):
    df1 = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        ["id", "name"],
    )

    df2 = spark.createDataFrame(
        [(3, "Charlie"), (4, "Diana")],
        ["id", "name"],
    )

    result = df1.union(df2)

    assert result.count() == 4


# ===========================================================================
# 18. LIMIT
# ===========================================================================


def test_limit(employees):
    result = employees.orderBy(F.col("salary").desc()).limit(3)

    assert result.count() == 3

    names = [row["name"] for row in result.collect()]

    assert names == ["Eve", "Alice", "Charlie"]


# ===========================================================================
# 19. WINDOW FUNCTIONS
# ===========================================================================


def test_window_function(employees):
    window = Window.partitionBy("department").orderBy(F.col("salary").desc())

    result = employees.withColumn(
        "department_rank",
        F.row_number().over(window),
    )

    eve = result.filter(F.col("name") == "Eve").first()
    bob = result.filter(F.col("name") == "Bob").first()

    assert eve["department_rank"] == 1
    assert bob["department_rank"] == 3


# ===========================================================================
# 20. WINDOW + AGGREGATE
# ===========================================================================


def test_window_sum(employees):
    window = Window.partitionBy("department")

    result = employees.withColumn(
        "department_total_salary",
        F.sum("salary").over(window),
    )

    alice = result.filter(F.col("name") == "Alice").first()
    frank = result.filter(F.col("name") == "Frank").first()

    assert alice["department_total_salary"] == 305000
    assert frank["department_total_salary"] == 135000


# ===========================================================================
# 21. COLLECT / ROW ACCESS
# ===========================================================================


def test_collect(employees):
    rows = (
        employees.filter(F.col("department") == "HR").select("name", "salary").collect()
    )

    result = {row["name"]: row["salary"] for row in rows}

    assert result == {
        "Frank": 70000,
        "Grace": 65000,
    }


# ===========================================================================
# 22. COMPLETE TRANSFORMATION CHAIN
# ===========================================================================


def test_transformation_chain(employees):
    """
    This is closer to how real PySpark code often looks:
    several transformations chained together.
    """

    result = (
        employees.filter(F.col("salary") >= 70000)
        .withColumn(
            "salary_k",
            F.col("salary") / 1000,
        )
        .groupBy("department")
        .agg(
            F.round(F.avg("salary_k"), 2).alias("avg_salary_k"),
            F.count("*").alias("employee_count"),
        )
        .filter(F.col("employee_count") >= 2)
        .orderBy(F.col("avg_salary_k").desc())
    )

    rows = result.collect()

    assert len(rows) == 2

    departments = {row["department"] for row in rows}

    assert departments == {"Engineering", "Sales"}
