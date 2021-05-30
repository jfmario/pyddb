
FROM alpine

RUN apk update && apk upgrade
RUN apk add python3 && \
    python3 -m ensurepip

ADD . /app

WORKDIR app

RUN python3 -m pip install -r requirements.txt

ENTRYPOINT python3 .