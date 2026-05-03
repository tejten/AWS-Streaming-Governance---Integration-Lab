.PHONY: local-demo test clean

local-demo:
	python3 -m src.local_demo.run_demo \
		--orders sample-data/orders_cdc.jsonl \
		--shipments sample-data/partner_shipments.jsonl \
		--out build/demo

test:
	python3 -m unittest discover -s tests

clean:
	rm -rf build
