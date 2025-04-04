package main

import (
	"context"
	"crypto/tls"

	"github.com/sirupsen/logrus"
	wire "github.com/stackql/psql-wire"
)

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	logger := logrus.New()

	cert, err := tls.LoadX509KeyPair("./psql.crt", "./psql.key")
	if err != nil {
		return err
	}

	certs := []tls.Certificate{cert}
	server, err := wire.NewServer(wire.SimpleQuery(handle), wire.Certificates(certs), wire.Logger(logger), wire.MessageBufferSize(100))
	if err != nil {
		return err
	}

	logger.Info("PostgreSQL server is up and running at [127.0.0.1:5432]")
	return server.ListenAndServe("127.0.0.1:5432")
}

func handle(ctx context.Context, query string, writer wire.DataWriter) error {
	return writer.Complete("", "OK")
}
