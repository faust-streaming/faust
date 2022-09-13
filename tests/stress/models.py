import random
from datetime import datetime, timezone
from itertools import count
from typing import Optional

import faust


class Withdrawal(faust.Record, isodates=True, serializer="json"):
    user: str
    country: str
    amount: float
    date: Optional[datetime] = None


def generate_withdrawals(n: Optional[int] = None):
    for d in generate_withdrawals_dict(n):
        yield Withdrawal(**d)


def generate_withdrawals_dict(n: Optional[int] = None):
    num_countries = 5
    countries = [f"country_{i}" for i in range(num_countries)]
    country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
    num_users = 500
    users = [f"user_{i}" for i in range(num_users)]
    for _ in range(n) if n is not None else count():
        yield {
            "user": random.choice(users),
            "amount": random.uniform(0, 25_000),
            "country": random.choices(countries, country_dist)[0],
            "date": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        }
