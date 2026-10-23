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
JAVA_SRC = $(shell find modules/*/src -type f -name "*.java" 2>/dev/null)
JAVA_TST = $(shell find modules/*/tst -type f -name "*.java" 2>/dev/null)

################################################################################
# Targets
################################################################################
.PHONY: all format clean clean-all

all: $(OUT)/src/ $(OUT)/tst/

$(OUT)/src/: $(JAVA_SRC)
	@ mkdir -p $@
	@ if [ -n "$(strip $(JAVA_SRC))" ]; then \
	    echo "[javac] $@"; \
	    $(JC) -d $@ -cp "$(SRC_JARS)" $(JAVA_SRC); \
	  fi
	@ touch $@

$(OUT)/tst/: $(JAVA_TST) $(OUT)/src
	@ mkdir -p $@
	@ if [ -n "$(strip $(JAVA_TST))" ]; then \
	    echo "[javac] $@"; \
	    $(JC) -d $@ -cp "$(TST_JARS)$(SEP)$(OUT)/src" $(JAVA_TST); \
	  fi
	@ touch $@

format:
	@ echo "[format]"
	@ java -jar jars/google-java-format.jar -i $(JAVA_SRC)

clean:
	@ echo "[clean]"
	@ rm -rf $(OUT)

clean-all: clean
	@ echo "[clean-all]"
	@ rm -rf traces
