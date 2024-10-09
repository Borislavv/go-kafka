## Kafka client based on Sarama (Shopify)

### Certs embedded filesystem:
1. **cd: {root}/certs/dir/in/your/project**

2. **mkdir certs**

3. **cp /path/to/certs/on/host/\* certs**

4. **touch fs.go**

5. **Pass the code into the created fs.go**


		package kafkacerts

    	import "embed"
    
    	//go:embed certs/*.crt
    	var EmbeddedFS embed.FS

6. You must have such structure:

    	// pwd: {root}/certs/dir/in/your/project
   		.certs
   		fs.go

### Usage:
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

	kafkaCfg := kafkaconfig.Kafka{}
	if err := envcofig("", &kafkaCfg); err != nil {
		logger.JsonRawLog("kafka: unable to init config", loggerenum.FatalLvl, err)
	}

	// go get github.com/Borislavv/go-logger
	loggerCfg := loggerconfig.Config{}
	if err := envcofig("", &loggerCfg); err != nil {
		logger.JsonRawLog("logger: unable to init config", loggerenum.FatalLvl, err)
		return
	}

	out, cancel, err := logger.NewOutput(loggerCfg)
	if err != nil {
		logger.JsonRawLog("logger: unable to init output", loggerenum.FatalLvl, err)
		return
	}
	defer cancel()

	lgr, cancel, err := logger.NewLogrus(loggerCfg, out)
	if err != nil {
		logger.JsonRawLog("logger: unable to init logrus", loggerenum.FatalLvl, err)
		return
	}
	defer cancel()

	client, err := kafka.New(ctx, kafkaCfg, lgr, kafkacerts.EmbeddedFS)
	if err != nil {
		logger.JsonRawLog("kafka: unable to init client (producer/consumer)", loggerenum.FatalLvl, err)
		return
	}
    defer func() {
        if err = client.Close(); err != nil {
            logger.JsonRawLog("kafka: close error", loggerenum.FatalLvl, err)
        }
    } 

	go func() {
		topics := []string{"example-topic", "another-example-topic"}

		for msg := range client.Consume(ctx, topics) {
			lgr.InfoMsg(ctx, string(msg.Value), nil)
		}
	}()

	if err = client.Produce("example-topic", "hello-world-from-example-topic"); err != nil {
		logger.JsonRawLog("kafka: produce error", loggerenum.FatalLvl, err)
	}

	if err = client.Produce("another-example-topic", "hello-world-from-another-example-topic"); err != nil {
		logger.JsonRawLog("kafka: produce error", loggerenum.FatalLvl, err)
	}
