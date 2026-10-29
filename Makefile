################################################################################
# Java compiler settings
################################################################################
JC = javac -source 17 -g

################################################################################
# Folders, files
################################################################################
OUT = out

# Windows uses ; as a path separator while Linux and Mac use :
SEP = :
ifeq ($(OS),Windows_NT)
	SEP = ;
endif

SRC_JARS = jars/framework.jar$(SEP)jars/framework-deps.jar
TST_JARS = $(SRC_JARS)$(SEP)jars/grader.jar$(SEP)jars/grader-deps.jar

# Every module keeps its sources in modules/<name>/src and tests in modules/<name>/tst
JAVA_SRC = $(shell find modules/*/src -type f -name "*.java" -not -path "modules/gateway/*" 2>/dev/null)
# The gateway runs the cluster on the test framework's runtime (RunState lives in the grader jar),
# so its sources are compiled together with the tests, against the same classpath.
JAVA_TST = $(shell find modules/*/tst modules/gateway/src -type f -name "*.java" 2>/dev/null)

################################################################################
# Targets
################################################################################
.PHONY: all format clean clean-all

all: $(OUT)/src/ $(OUT)/tst/

$(OUT)/src/: $(JAVA_SRC)
	@ mkdir -p $@
	@ if [ -n "$(strip $(JAVA_SRC))" ]; then \
	    echo "[javac] $@"; \
	    $(JC) -d $@ -cp "$(SRC_JARS)" $(JAVA_SRC) || { rm -rf "$@"; exit 1; }; \
	  fi
	@ touch $@

$(OUT)/tst/: $(JAVA_TST) $(OUT)/src
	@ mkdir -p $@
	@ if [ -n "$(strip $(JAVA_TST))" ]; then \
	    echo "[javac] $@"; \
	    $(JC) -d $@ -cp "$(TST_JARS)$(SEP)$(OUT)/src" $(JAVA_TST) || { rm -rf "$@"; exit 1; }; \
	  fi
	@ touch $@

# Run the HTTP gateway: make run-gateway ARGS="--port 8080 --groups 2 --replicas 3"
.PHONY: run-gateway
run-gateway: all
	@ java --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED \
	    --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED \
	    --add-opens java.base/java.time=ALL-UNNAMED \
	    -cp "$(TST_JARS)$(SEP)$(OUT)/src$(SEP)$(OUT)/tst" cmws.gateway.GatewayMain $(ARGS)

# The workspace engine (Python, standard library only). Its integration tests start the gateway.
.PHONY: test-workspace
test-workspace: all
	@ cd workspace && PYTHONPATH=src python3 -m unittest discover -s tests -t .

format:
	@ echo "[format]"
	@ java -jar jars/google-java-format.jar -i $(JAVA_SRC)

clean:
	@ echo "[clean]"
	@ rm -rf $(OUT)

clean-all: clean
	@ echo "[clean-all]"
	@ rm -rf traces
