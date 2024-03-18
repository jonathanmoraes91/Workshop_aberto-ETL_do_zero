FROM python:3.12
RUN pip installl poetry
COPY . /src
WORKDIR /src
RUN poetry installl
EXPOSE 8501
ENTRYPOINT ["poetry", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0" ]
# ENTRYPOINT ["poetry", "run", "python", "main.py"]