# ml-mock

Mock consumer. Reads the delivered encrypted Gold partition, checks the schema
against the feature contract, and simulates retraining. Also hosts an always-on
Streamlit delivery dashboard (host port 8501) reading only the `delivered`
bucket.