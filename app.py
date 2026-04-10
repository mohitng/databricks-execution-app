from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

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
    from databricks.sdk import WorkspaceClient
    import time

    w = WorkspaceClient()
    execution_id = req.execution_id

    safe_execution_id = execution_id.replace("'", "''")

    # ✅ FIX: fully qualified table name (NO USE statements)
    query = f"""
    SELECT query_file, output_path
    FROM app_catalog.app_schema.execution_metadata
    WHERE execution_id = '{safe_execution_id}' AND is_active = true
    """

    try:
        # Step 1: Submit query
        statement = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query
        )

        # Step 2: Poll until completion
        result = w.statement_execution.get_statement(statement.statement_id)

        while result.status.state in ["PENDING", "RUNNING"]:
            time.sleep(1)
            result = w.statement_execution.get_statement(statement.statement_id)

        # Step 3: Check status
        if result.status.state != "SUCCEEDED":
            raise HTTPException(
                status_code=500,
                detail=f"Query failed: {result.status.error}"
            )

        # Step 4: Validate result
        if not result.result or not result.result.data_array:
            raise HTTPException(
                status_code=400,
                detail="Invalid execution_id"
            )

        # Step 5: Extract values
        row = result.result.data_array[0]
        query_file = row[0]
        output_path = row[1]

        # Step 6: Trigger job
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

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
