FROM golang:1.23

WORKDIR /home/isucon/webapp/go

RUN apt-get update && apt-get install --no-install-recommends -y \
  default-mysql-client-core=1.1.0 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

COPY . .
# service.version 用。compose から GIT_COMMIT を渡す (未指定なら付けない)
ARG GIT_COMMIT=
RUN go build -ldflags "-X main.buildVersion=${GIT_COMMIT}" -o webapp .

CMD ["/home/isucon/webapp/go/webapp"]
