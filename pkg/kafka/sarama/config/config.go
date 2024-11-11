package kafkasaramaconfig

import (
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/pem"
	"errors"
	"fmt"
	kafkaconfiginterface "github.com/Borislavv/go-kafka/pkg/kafka/config/interface"
	saslscram "github.com/Borislavv/go-kafka/pkg/kafka/sasl/scram"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
	"io/fs"
	"path/filepath"
)

func New(cfg kafkaconfiginterface.Configurator, certsFS ...embed.FS) (config *sarama.Config, err error) {
	config = sarama.NewConfig()

	if err = setUpSASLConfig(config, cfg); err != nil {
		return nil, err
	}

	if err = setUpTLSConfig(config, cfg, certsFS...); err != nil {
		return nil, err
	}

	setUpGroupConfig(config)
	setUpOffsetConfig(config)

	return config, nil
}

func setUpSASLConfig(config *sarama.Config, cfg kafkaconfiginterface.Configurator) error {
	switch cfg.GetSASLMechanism() {
	case sarama.SASLTypePlaintext:
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = cfg.GetSASLUser()
		config.Net.SASL.Password = cfg.GetSASLPassword()
	case sarama.SASLTypeSCRAMSHA256:
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		config.Net.SASL.User = cfg.GetSASLUser()
		config.Net.SASL.Password = cfg.GetSASLPassword()
		scramClient := &saslscram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA256}
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return scramClient }
	case sarama.SASLTypeSCRAMSHA512:
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.GetSASLUser()
		config.Net.SASL.Password = cfg.GetSASLPassword()
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.Handshake = true

		scramClient := &saslscram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return scramClient }
	default:
		return fmt.Errorf("invalid sasl mechanism: %v unsupported", cfg.GetSASLMechanism())
	}

	return nil
}

func setUpTLSConfig(config *sarama.Config, cfg kafkaconfiginterface.Configurator, certsFS ...embed.FS) error {
	if !cfg.GetTLSEnabled() {
		return nil
	}

	config.Net.TLS.Enable = true

	var pemCerts [][]byte

	for _, certFS := range certsFS {
		dir, err := certFS.ReadDir(cfg.GetCertsDir())
		if err != nil {
			return fmt.Errorf("read certs dir: %w", err)
		}
		for _, entry := range dir {
			f, err := fs.ReadFile(certFS, filepath.Join(cfg.GetCertsDir(), entry.Name()))
			if err != nil {
				return fmt.Errorf("read %s: %w", entry.Name(), err)
			}

			pemCerts = append(pemCerts, f)
		}
	}

	if len(pemCerts) == 0 {
		return nil
	}

	var err error
	config.Net.TLS.Config = &tls.Config{}
	config.Net.TLS.Config.RootCAs, err = x509.SystemCertPool()
	if err != nil {
		return fmt.Errorf("system cert pool: %w", err)
	}

	for _, pemCert := range pemCerts {
		cpem, _ := pem.Decode(pemCert)
		if cpem == nil {
			return errors.New("CA cert is not PEM")
		}
		if cpem.Type != "CERTIFICATE" {
			return fmt.Errorf("unknown PEM type '%s', expected CERTIFICATE", cpem.Type)
		}

		cert, err := x509.ParseCertificate(cpem.Bytes)
		if err != nil {
			return fmt.Errorf("parse CA cert: %w", err)
		}

		config.Net.TLS.Config.RootCAs.AddCert(cert)
	}

	return nil
}

func setUpGroupConfig(config *sarama.Config) {
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Group.Rebalance.Timeout = config.Consumer.Group.Session.Timeout
}

func setUpOffsetConfig(config *sarama.Config) {
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
}
