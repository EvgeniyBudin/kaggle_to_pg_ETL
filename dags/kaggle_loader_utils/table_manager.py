import json
import os
import pandas as pd
import psycopg2.extras


CUR_DIR = os.path.abspath(os.path.dirname(__file__))
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


class TableManager:
    def __init__(self, nrows: int = 100000):
        self.nrows = nrows

    def load_to_pg(self, path: str) -> None:
        self.full = pd.read_csv(os.path.join(CUR_DIR, path.replace(" ", "%20")), nrows=self.nrows)
        self.branches = self.load_branches()
        self.insert_branches()
        self.branch_ids = self.get_branches_id()
        self.supercenters = self.load_supercenters()
        self.insert_supercenters()
        self.supercenters_ids = self.get_supercenters_id()
        self.customer_types = self.load_customer_types()
        self.insert_customer_types()
        self.customer_types_ids = self.get_customer_types_id()
        self.customers = self.load_customers()
        self.insert_customers()
        self.customers_ids = self.get_customers_id()
        self.product_lines = self.load_product_lines()
        self.insert_product_lines()
        self.product_lines_ids = self.get_product_lines_id()
        self.products = self.load_products()
        self.insert_products()
        self.products_ids = self.get_products_id()
        self.invoices = self.load_invoices()
        self.insert_invoices()

    def load_branches(self):
        return self.full.Branch.unique().tolist()

    def insert_branches(self):
        args_str = b",".join(cur.mogrify("(%s)", x) for x in self.branches)
        cur.execute(
            b"INSERT INTO branches(Branch) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()

    def get_branches_id(self):
        sql = cur.mogrify(
            "SELECT id, Branch from branches WHERE Branch IN %s",
            (tuple(self.branches),),
        )
        cur.execute(sql)
        return pd.read_json(json.dumps(cur.fetchall()))

    def load_supercenters(self):
        supercenters = self.full[["Branch", "City"]]
        supercenters = supercenters.merge(
            self.branch_ids, how="left", left_on="Branch", right_on="branch"
        )
        return (
            supercenters[["id", "City"]]
            .rename(columns={"id": "Branch_id"})
            .drop_duplicates()
        )

    def insert_supercenters(self):
        args_str = b",".join(
            cur.mogrify("(%s, %s)", x)
            for x in self.supercenters.itertuples(index=False, name=None)
        )
        cur.execute(
            b"INSERT INTO Supercenters(Branch_id, City) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()

    def get_supercenters_id(self):
        sql = cur.mogrify(
            "SELECT id, City from Supercenters WHERE City IN %s",
            (tuple(self.supercenters.City),),
        )
        cur.execute(sql)
        return pd.read_json(json.dumps(cur.fetchall()))

    def load_customer_types(self):
        return self.full["Customer type"].unique().tolist()

    def insert_customer_types(self):
        args_str = b",".join(cur.mogrify("(%s)", (x,)) for x in self.customer_types)
        cur.execute(
            b"INSERT INTO Customer_types(type) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()

    def get_customer_types_id(self):
        sql = cur.mogrify(
            "SELECT id, type from Customer_types WHERE type IN %s",
            (tuple(self.customer_types),),
        )
        cur.execute(sql)
        return pd.read_json(json.dumps(cur.fetchall()))

    def load_customers(self):
        customers = self.full[["Customer type", "Gender", "Rating"]]
        customers = customers.merge(
            self.customer_types_ids,
            how="left",
            left_on="Customer type",
            right_on="type",
        )
        return (
            customers[["id", "Gender", "Rating"]]
            .rename(columns={"id": "Type_id"})
            .drop_duplicates()
        )

    def insert_customers(self):
        args_str = b",".join(
            cur.mogrify("(%s, %s, %s)", x)
            for x in self.customers.itertuples(index=False, name=None)
        )
        cur.execute(
            b"INSERT INTO Customers(Type_id, Gender, Rating) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()

    def get_customers_id(self):
        sql = cur.mogrify(
            "SELECT id, Rating from Customers WHERE Rating IN %s",
            (tuple(self.customers.Rating),),
        )
        cur.execute(sql)
        return pd.read_json(json.dumps(cur.fetchall()))

    def load_product_lines(self):
        return self.full["Product line"].unique().tolist()

    def insert_product_lines(self):
        args_str = b",".join(cur.mogrify("(%s)", (x,)) for x in self.product_lines)
        cur.execute(
            b"INSERT INTO Product_lines(Line) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()

    def get_product_lines_id(self):
        sql = cur.mogrify(
            "SELECT id, Line from Product_lines WHERE Line IN %s",
            (tuple(self.product_lines),),
        )
        cur.execute(sql)
        return pd.read_json(json.dumps(cur.fetchall()))

    def load_products(self):
        products = self.full[["Product line", "Unit price"]]
        products = products.merge(
            self.product_lines_ids,
            how="left",
            left_on="Product line",
            right_on="line",
        )
        return (
            products[["id", "Unit price"]]
            .rename(columns={"id": "line_id"})
            .drop_duplicates()
        )

    def insert_products(self):
        args_str = b",".join(
            cur.mogrify("(%s, %s)", x)
            for x in self.products.itertuples(index=False, name=None)
        )
        cur.execute(
            b"INSERT INTO Products(line_id, Unit_price) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()

    def get_products_id(self):
        sql = cur.mogrify(
            "SELECT id, Unit_price from Products WHERE Unit_price IN %s",
            (tuple(self.products["Unit price"]),),
        )
        cur.execute(sql)
        return pd.read_json(json.dumps(cur.fetchall()))

    def load_invoices(self):
        invoices = self.full[
            [
                "Invoice ID",
                "City",
                "Rating",
                "Unit price",
                "Quantity",
                "Tax 5%",
                "Total",
                "Date",
                "Time",
                "Payment",
                "cogs",
                "gross margin percentage",
                "gross income",
            ]
        ]
        invoices = invoices.merge(
            self.supercenters_ids,
            how="left",
            left_on="City",
            right_on="city",
        ).rename(columns={"id": "Supercenter_id"})
        invoices = invoices.merge(
            self.customers_ids,
            how="left",
            left_on="Rating",
            right_on="rating",
        ).rename(columns={"id": "Customer_id"})
        invoices = invoices.merge(
            self.products_ids,
            how="left",
            left_on="Unit price",
            right_on="unit_price",
        ).rename(columns={"id": "Product_id"})
        invoices["Datetime"] = pd.to_datetime(
            invoices["Date"] + " " + invoices["Time"], format="%m/%d/%Y %H:%M"
        )
        return invoices[
            [
                "Invoice ID",
                "Supercenter_id",
                "Customer_id",
                "Product_id",
                "Quantity",
                "Tax 5%",
                "Total",
                "Datetime",
                "Payment",
                "cogs",
                "gross margin percentage",
                "gross income",
            ]
        ].drop_duplicates()

    def insert_invoices(self):
        args_str = b",".join(
            cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x)
            for x in self.invoices.itertuples(index=False, name=None)
        )
        cur.execute(
            b"INSERT INTO Invoices(Invoice_id, Supercenter_id, Customer_id, Product_id, Quantity, Tax, Total, Datetime, Payment, cogs, margin_percentage, income) VALUES "
            + args_str
            + b"ON CONFLICT DO NOTHING"
        )
        conn.commit()
