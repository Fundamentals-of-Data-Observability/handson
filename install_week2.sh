cd python_environment

docker build . --build-arg project=$1 -t mypython

cd volume/week2
if [ ! -d "airbyte" ]; then
  git clone -b v0.40.4 https://github.com/airbytehq/airbyte.git
fi

cd ..
cd ..

docker run -it --rm -v $(pwd)/volume:/volume mypython /bin/bash 