.PHONY: up down run ingest-batch logs

up:
	docker compose up -d

down:
	docker compose down

run:
	@test -n "$(LAYER)" && test -n "$(JOB)" || \
		(printf 'Usage: make run LAYER=bronze JOB=ingest_blocks\\n' >&2; exit 1)
	./run.sh "$(LAYER)" "$(JOB)"

ingest-batch:
	@test -n "$(BATCH)" || \
		(printf 'Usage: make ingest-batch BATCH=YYYY-MM-DD\\n' >&2; exit 1)
	./run_batch.sh "$(BATCH)"

logs:
	docker compose logs -f
