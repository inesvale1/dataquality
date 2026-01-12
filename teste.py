import os
from pathlib import Path
from dq.app.use_cases.run_model_quality import RunOptions, run_model_quality


if __name__ == "__main__":
    print("Current working directory:", os.getcwd())
    print(f"Path: {Path(__file__).resolve()}")
    print(f"Path parent: {Path(__file__).resolve().parent}")
