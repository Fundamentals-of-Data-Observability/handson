cd python_environment

docker build . --build-arg project=$1 -t mypython

docker run -it --rm -v $(pwd)/volume:/volume mypython /bin/bash 