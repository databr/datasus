deploy:
	GOOS=linux GOARCH=amd64 go build -o pkg/crawler-datasus crawler.go
	scp pkg/crawler-datasus $(DATABR_BOT_MACHINE):/usr/local/bin/crawler-datasus
