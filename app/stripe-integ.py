import os
STRIPE_KEY = os.getenv("STRIPE_SECRET", "")

def create_payouts_with_stripe(payouts: list[dict]):
    # MVP: stub – später Stripe Payouts oder Bank-API
    return {"status": "stub", "count": len(payouts)}
