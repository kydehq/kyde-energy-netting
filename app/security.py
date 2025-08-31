from fastapi import Header, HTTPException

async def require_api_key(x_api_key: str = Header(...)):
    # Für MVP: ein globaler Key; später per Participant/Community
    # Setze ENV KYDE_API_KEY
    import os
    if x_api_key != os.getenv("KYDE_API_KEY"):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True
