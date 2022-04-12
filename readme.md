# miquella | [log](https://rakeqse.viole.in)

stream with `kafka`, serialize with `protobuf`, transform image with `opencv`

## requirements

```
docker kafka librdkafka python virtualenv
```

## installation

```
pip install -r requirements.txt
```

## start things up

1. spin up docker

```
docker-compose up -d
```

2. spin up producer

```
python prod.proto.py
```

3. spin up consumer

```
python prod.proto.py
```
