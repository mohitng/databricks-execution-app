from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

WAREHOUSE_ID = "76928b72e59d53fa"
JOB_ID = 1112246778869928


# Request model
class ExecuteRequest(BaseModel):
    execution_id: str


# ✅ Health endpoint
@app.get("/")
def home():
    return {"message": "Hello, Welcome to Mohit's Org Application 🚀"}


# ✅ Execute API
@app.post("/execute")
def execute(req: ExecuteRequest):
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    execution_id = req.execution_id

    # 🔒 Basic SQL safety
    safe_execution_id = execution_id.replace("'", "''")

    query = f"""
    SELECT query_file, output_path
    FROM my_catalog.my_schema.execution_metadata
    WHERE execution_id = '{safe_execution_id}' AND is_active = true
    """

    try:
        # Execute statement
        statement = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query
        )

        # Wait for result
        result = statement.result()

        # Validate result
        if not result:
            raise HTTPException(status_code=500, detail="Query execution failed")

        if not result.data_array:
            raise HTTPException(status_code=400, detail="Invalid execution_id")

        # Extract values
        row = result.data_array[0]
        query_file = row[0]
        output_path = row[1]

        # Trigger job
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

    except Exception as e:
        # Catch all unexpected errors
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Entry point for Databricks Apps
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
