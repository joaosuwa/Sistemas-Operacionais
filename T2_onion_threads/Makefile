# Makefile — Missão Orion
# INF01142 — Sistemas Operacionais — 2026/1

CC     = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c11 -g 
LDFLAGS = -lpthread -lm

TARGET = orion

all: $(TARGET)

$(TARGET): orion.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# Compilação sem sanitizers (para medir desempenho)
release: orion.c
	$(CC) -Wall -std=c11 -O2 -o $(TARGET) $< $(LDFLAGS)

# Execução de referência para observar falhas
run-buggy: all
	./$(TARGET) 5 8 4 20

# Execução para testar a solução correta
run: all
	./$(TARGET) 5 8 4 100

pack:
	mkdir -p T2_threads
	cp Makefile orion.c T2_threads/
	tar czf T2_threads.tgz T2_threads/
	rm -rf T2_threads

clean:
	rm -f $(TARGET)
