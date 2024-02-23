FROM python:3.10

ADD ./target/wheels/phenolrs-0.1.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl .

RUN pip install phenolrs-0.1.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
RUN pip install --extra-index-url https://download.pytorch.org/whl/cpu torch==2.1.0+cpu
RUN pip install -f https://data.pyg.org/whl/torch-2.1.0+cpu.html \
                torch-scatter==2.1.2 \
                torch-sparse==0.6.18 \
                torch-geometric==2.4.0 \
                pyg_lib==0.3.1

ADD benchmarks/local.py ./local.py

CMD ["python", "local.py", "--host", "http://host.docker.internal:8529", "test"]
