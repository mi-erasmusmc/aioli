.PHONY: run
run:
	mvn clean package assembly:single
	java -jar target/ailoi_java-1.0-SNAPSHOT-jar-with-dependencies.jar -tv rxnorm -rmin false -sn true
