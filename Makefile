DC := docker-compose

format:
	./gradlew goJF

compile: format
	./gradlew clean build

dist: format
	./gradlew clean distTar

.PHONY: clean docker format compile dist

docker: dist
	$(DC) rm && $(DC) build && $(DC) up -d

clean:
	$(DC) down -v
