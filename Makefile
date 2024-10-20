dc_up:
	docker compose up -d

dc_down:
	docker compose down -v

streamlit:
	streamlit run ./dags/streamlit.py
.PHONY: dc_up dc_down streamlit