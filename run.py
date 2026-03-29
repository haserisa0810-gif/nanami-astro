import uvicorn
import os

#if __name__ == "__main__":
#    uvicorn.run("routes:app", host="127.0.0.1", port=8000, reload=True)
    



if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("routes:app", host="0.0.0.0", port=port)
