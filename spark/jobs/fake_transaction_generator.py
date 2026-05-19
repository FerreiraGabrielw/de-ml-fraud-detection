import random
import uuid
from datetime import datetime
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine


# ============================================================
# CONFIGURATION
# ============================================================

fake = Faker()

POSTGRES_USER = "fraud_admin"
POSTGRES_PASSWORD = "fraud_password"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "fraud_detection"

TOTAL_TRANSACTIONS = 5000
FRAUD_RATIO = 0.05  # 5% fraud transactions


# ============================================================
# DATABASE CONNECTION
# ============================================================

engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


# ============================================================
# NORMAL TRANSACTION GENERATOR
# ============================================================

def generate_normal_transaction():

    transaction_time = fake.date_time_between(
        start_date="-1d",
        end_date="now"
    )

    customer_lat = round(random.uniform(30, 45), 6)
    customer_long = round(random.uniform(-100, -70), 6)

    merch_lat = customer_lat + random.uniform(-0.05, 0.05)
    merch_long = customer_long + random.uniform(-0.05, 0.05)

    amount = round(random.uniform(5, 300), 2)

    return {

        "trans_date_trans_time": transaction_time,

        "cc_num": fake.random_number(
            digits=16,
            fix_len=True
        ),

        "merchant": fake.company(),

        "category": random.choice([
            "grocery_pos",
            "shopping_pos",
            "gas_transport",
            "food_dining"
        ]),

        "amt": amount,

        "first_name": fake.first_name(),

        "last_name": fake.last_name(),

        "gender": random.choice(["M", "F"]),

        "street": fake.street_address(),

        "city": fake.city(),

        "state": fake.state_abbr(),

        "zip": fake.zipcode(),

        "lat": customer_lat,

        "long": customer_long,

        "city_pop": random.randint(1000, 1000000),

        "job": fake.job(),

        "dob": fake.date_of_birth(
            minimum_age=18,
            maximum_age=80
        ),

        "trans_num": str(uuid.uuid4()),

        "unix_time": int(transaction_time.timestamp()),

        "merch_lat": merch_lat,

        "merch_long": merch_long,

        "is_fraud": 0
    }


# ============================================================
# FRAUD TRANSACTION GENERATOR
# ============================================================

def generate_fraud_transaction():

    transaction_time = datetime.now().replace(
        hour=random.randint(1, 4),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0
    )

    customer_lat = round(random.uniform(30, 45), 6)
    customer_long = round(random.uniform(-100, -70), 6)

    # Large geographic discrepancy
    merch_lat = customer_lat + random.uniform(10, 20)
    merch_long = customer_long + random.uniform(10, 20)

    amount = round(random.uniform(2000, 10000), 2)

    return {

        "trans_date_trans_time": transaction_time,

        "cc_num": fake.random_number(
            digits=16,
            fix_len=True
        ),

        "merchant": fake.company(),

        "category": random.choice([
            "shopping_net",
            "misc_net",
            "electronics"
        ]),

        "amt": amount,

        "first_name": fake.first_name(),

        "last_name": fake.last_name(),

        "gender": random.choice(["M", "F"]),

        "street": fake.street_address(),

        "city": fake.city(),

        "state": fake.state_abbr(),

        "zip": fake.zipcode(),

        "lat": customer_lat,

        "long": customer_long,

        "city_pop": random.randint(1000, 1000000),

        "job": fake.job(),

        "dob": fake.date_of_birth(
            minimum_age=18,
            maximum_age=80
        ),

        "trans_num": str(uuid.uuid4()),

        "unix_time": int(transaction_time.timestamp()),

        "merch_lat": merch_lat,

        "merch_long": merch_long,

        "is_fraud": 1
    }


# ============================================================
# GENERATING TRANSACTION BATCH
# ============================================================

transactions = []

fraud_transactions = int(
    TOTAL_TRANSACTIONS * FRAUD_RATIO
)

normal_transactions = (
    TOTAL_TRANSACTIONS - fraud_transactions
)

print(f"Generating {normal_transactions} normal transactions...")
print(f"Generating {fraud_transactions} fraudulent transactions...")


for _ in range(normal_transactions):
    transactions.append(
        generate_normal_transaction()
    )


for _ in range(fraud_transactions):
    transactions.append(
        generate_fraud_transaction()
    )


# Shuffling dataset
random.shuffle(transactions)

# Creating Pandas DataFrame
df = pd.DataFrame(transactions)

print(f"Generated dataset size: {len(df):,}")


# ============================================================
# WRITING DATA INTO POSTGRESQL
# ============================================================

print("Writing transactions into PostgreSQL...")

df.to_sql(
    "transactions",
    engine,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=1000
)

print("Fake transaction batch successfully inserted.")