FROM python:3.10

RUN pip install -f https://data.pyg.org/whl/torch-2.1.0+cpu.html \
                torch-scatter==2.1.2 \
                torch-sparse==0.6.18 \
                torch-geometric==2.4.0 \
                pyg_lib==0.3.1
RUN pip install --extra-index-url https://download.pytorch.org/whl/cpu torch==2.1.0+cpu

ADD ./target/wheels/ ./target/wheels
RUN pip install phenolrs --find-links ./target/wheels

ADD benchmarks/local.py ./local.py

CMD ["python", "local.py", "--host", "http://host.docker.internal:8529", "test"]
