from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List, Any
from pydantic import BaseModel

app = FastAPI(title="Mock Transactions API")

class Transaction(BaseModel):
    id: int
    user_id: str
    amount: float
    created_at: str

class PaginatedTransactionsResponse(BaseModel):
    data: List[Transaction]
    next_page: Optional[int]

TOTAL_PAGES = 10
RECORDS_PER_PAGE = 100

@app.get("/api/transactions", response_model=PaginatedTransactionsResponse)
def get_transactions(page: int = Query(1, description="Page number")):
    if page < 1 or page > TOTAL_PAGES:
        return PaginatedTransactionsResponse(data=[], next_page=None)
    
    start_id = (page - 1) * RECORDS_PER_PAGE + 1
    end_id = start_id + RECORDS_PER_PAGE

    data = []
    for i in range(start_id, end_id):
        # Generate predictable transaction record
        user_num = (i % 50) + 1
        amount = round(10.50 + (i * 1.75) % 500, 2)
        # Create timestamp in ISO8601 format
        day = (i % 28) + 1
        hour = (i % 24)
        minute = (i % 60)
        created_at = f"2024-01-{day:02d}T{hour:02d}:{minute:02d}:00Z"
        
        data.append(
            Transaction(
                id=i,
                user_id=f"user_{user_num}",
                amount=amount,
                created_at=created_at
            )
        )
    
    next_page = page + 1 if page < TOTAL_PAGES else None
    return PaginatedTransactionsResponse(data=data, next_page=next_page)

@app.get("/health")
def health_check():
    return {"status": "ok"}
