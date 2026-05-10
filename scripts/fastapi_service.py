from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends

app = FastAPI()

API_KEY = "<create_unique_key>"
BASE_PATH = "<full_path_of_the_csv_folder>"

security = HTTPBearer()

# ================================
# AUTH VALIDATION
# ================================
def validate_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ================================
# FILE DISCOVERY ENDPOINT (DEFINE FIRST)
# ================================
@app.get("/files")
def list_available_files(credentials: HTTPAuthorizationCredentials = Depends(security)):
    validate_api_key(credentials)

    result = {}

    for date_folder in os.listdir(BASE_PATH):
        date_path = os.path.join(BASE_PATH, date_folder)

        if not os.path.isdir(date_path):
            continue

        for file in os.listdir(date_path):

            if not file.endswith(".csv"):
                continue

            file_name = file.replace(".csv", "")
            parts = file_name.split("_")

            source = "_".join(parts[:-1])
            date = parts[-1]

            if source not in result:
                result[source] = []

            result[source].append(date)

    # Remove duplicates + sort
    for source in result:
        result[source] = sorted(list(set(result[source])))

    return JSONResponse(content=result)


# ================================
# GENERIC FILE READER (DEFINE AFTER)
# ================================
@app.get("/{source}")
def get_file(source: str, date: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    validate_api_key(credentials)

    file_path = f"{BASE_PATH}/{date}/{source}_{date}.csv"

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        file_path,
        media_type="text/csv",
        filename=f"{source}_{date}.csv"
    )