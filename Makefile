format:
	./gradlew clean goJF

compile: format
	./gradlew clean build

assembly: format
	./gradlew clean distTar

docker: format
	docker-compose rm && docker-compose build && docker-compose up -d
