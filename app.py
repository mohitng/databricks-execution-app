from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

JOB_ID = 1112246778869928


class ExecuteRequest(BaseModel):
    execution_id: str


@app.get("/")
def home():
    return {"message": "Hello, Welcome to Mohit's Org Application 🚀"}


@app.post("/execute")
def execute(req: ExecuteRequest):
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    try:
        run = w.jobs.run_now(
            job_id=JOB_ID,
            notebook_params={
                "execution_id": req.execution_id
            }
        )

        return {
            "status": "triggered",
            "run_id": run.run_id
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
