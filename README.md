# CS422-HW02

## Running with Docker

Build the image:

```sh
docker build -t cs422-hw02 .
```

Host folder mounted at `/usr/local/app/results` is used to write TCP samples and summary table (and TODO pdf graphs). Args are passed to the script:

```sh
mkdir -p results
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/results:/usr/local/app/results" \
  cs422-hw02 --duration 60 --num-tests 10 --algorithms cubic,reno,bbr
```

You have to create `results/` before running or else docker creates it and it breaks everything since the folder isn't writable (docker creates it owned by root).