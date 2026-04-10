from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

app = FastAPI()

WAREHOUSE_ID = "76928b72e59d53fa"
JOB_ID = 1112246778869928


class ExecuteRequest(BaseModel):
    execution_id: str


@app.get("/")
def home():
    return {"message": "Hello, Welcome to Mohit's Org Application 🚀"}


@app.post("/execute")
def execute(req: ExecuteRequest):
    # ✅ Initialize here (NOT at top)
    w = WorkspaceClient()

    execution_id = req.execution_id

    query = f"""
    SELECT query_file, output_path
    FROM my_catalog.my_schema.execution_metadata
    WHERE execution_id = '{execution_id}' AND is_active = true
    """

    res = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query
    ).result

    if not res.data_array:
        raise HTTPException(status_code=400, detail="Invalid execution_id")

    row = res.data_array[0]
    query_file = row[0]
    output_path = row[1]

    run = w.jobs.run_now(
        job_id=JOB_ID,
        notebook_params={
            "query_file": query_file,
            "output_path": output_path
        }
    )

    return {
        "status": "triggered",
        "run_id": run.run_id
    }
