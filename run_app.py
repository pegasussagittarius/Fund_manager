# run_app.py
import os, sys, subprocess
def main():
    here = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(here, "app.py")
    # Gọi: python -m streamlit run app.py
    subprocess.run([sys.executable, "-m", "streamlit", "run", app_path, "--server.headless=false"])
if __name__ == "__main__":
    main()
