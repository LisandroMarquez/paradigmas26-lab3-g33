all: run 

JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64
SUBSCRIPTIONS := data/local_subscriptions.json
ENTITIES := data/valid_entities
TOPK := 15

run:
	JAVA_HOME=$(JAVA_HOME) \
	PATH="$(JAVA_HOME)/bin:$$PATH" \
	SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
	sbt "run --subscription-file $(SUBSCRIPTIONS) --entities-dir $(ENTITIES) --top-k $(TOPK)"