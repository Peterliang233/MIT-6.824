.PHONY: clean
master:
	go run mrcoordinator.go pg-being_ernest.txt pg-grimm.txt pg-sherlock_holmes.txt pg-dorian_gray.txt pg-huckleberry_finn.txt pg-tom_sawyer.txt pg-frankenstein.txt pg-metamorphosis.txt
wc.so:
	go build -buildmode=plugin ../mrapps/wc.go
worker: wc.so
	go run mrworker.go wc.so
update:
	go build -buildmode=plugin ../mrapps/wc.go
clean:
	rm -rf mr-*