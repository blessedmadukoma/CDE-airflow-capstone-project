dc_up:
	docker compose up -d

dc_down:
	docker compose down -v

.PHONY: dc_up dc_down