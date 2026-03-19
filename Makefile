# Makefile — Mandelbrot Paralelo
# INF01142 — Sistemas Operacionais — 2026/1
#
# Uso:
#   make                        → compila (detecta RayLib automaticamente)
#   make RAYLIB_PREFIX=/caminho → compila usando RayLib em /caminho/lib e /caminho/include
#   make clean                  → remove binários e objetos
#   make run                    → compila e executa com parâmetros de exemplo
#   make run-zoom               → compila e executa com zoom em espiral
#
# Exemplos com prefixo explícito:
#   make RAYLIB_PREFIX=/usr/local
#   make RAYLIB_PREFIX=$(HOME)/.local
#   make RAYLIB_PREFIX=/opt/raylib-5.0

CC      = gcc
CFLAGS  = -Wall -Wextra -pedantic -std=c99 -O2
LDFLAGS = -lm

# --- RayLib (linkagem dinâmica) ---

# Dependências de sistema por plataforma
UNAME := $(shell uname -s)
ifeq ($(UNAME), Linux)
    RAYLIB_SYSLIBS = -lGL -lm -lpthread -ldl -lrt -lX11
else ifeq ($(UNAME), Darwin)
    RAYLIB_SYSLIBS = -framework OpenGL -framework Cocoa -framework IOKit \
                     -framework CoreVideo -framework CoreFoundation
else
    $(error Sistema operacional não suportado: $(UNAME))
endif

# RAYLIB_PREFIX — defina para apontar para a instalação da RayLib.
# Espera-se que existam $(RAYLIB_PREFIX)/lib e $(RAYLIB_PREFIX)/include.
# Se não definido, a detecção automática (pkg-config / caminhos padrão) é usada.
RAYLIB_PREFIX ?= /home/schnorr/install/raylib/

ifneq ($(RAYLIB_PREFIX),)
    # Prefixo explícito: usa $(RAYLIB_PREFIX)/include e $(RAYLIB_PREFIX)/lib
    RAYLIB_CFLAGS  := -I$(RAYLIB_PREFIX)/include
    RAYLIB_LDFLAGS := -L$(RAYLIB_PREFIX)/lib -lraylib $(RAYLIB_SYSLIBS)
else
    # Detecção automática: pkg-config primeiro, depois caminhos convencionais
    PKGCONFIG_OK := $(shell pkg-config --exists raylib 2>/dev/null && echo yes)
    ifeq ($(PKGCONFIG_OK), yes)
        RAYLIB_CFLAGS  := $(shell pkg-config --cflags raylib)
        RAYLIB_LDFLAGS := -lraylib $(RAYLIB_SYSLIBS)
    else
        RAYLIB_SEARCH := $(firstword $(wildcard \
            /usr/local/lib/libraylib.so \
            /usr/local/lib/libraylib.dylib \
            /usr/lib/libraylib.so \
            /usr/lib/x86_64-linux-gnu/libraylib.so))
        ifeq ($(RAYLIB_SEARCH),)
            $(error RayLib não encontrada. Use: make RAYLIB_PREFIX=/caminho/para/raylib)
        endif
        RAYLIB_CFLAGS  := $(addprefix -I, $(wildcard /usr/local/include /usr/include))
        RAYLIB_LDFLAGS := -lraylib $(RAYLIB_SYSLIBS)
    endif
endif

# --- Alvos ---
TARGET  = mandelbrot
SOURCES = mandelbrot.c ipc.c colors.c
OBJECTS = $(SOURCES:.c=.o)

.PHONY: all clean run run-zoom

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $(OBJECTS) $(RAYLIB_LDFLAGS) $(LDFLAGS)
	@echo ""
	@echo "  Compilado com sucesso: ./$(TARGET)"
	@echo "  Exemplo: ./$(TARGET)  ou  ./$(TARGET) 1200 900"
	@echo ""

%.o: %.c
	$(CC) $(CFLAGS) $(RAYLIB_CFLAGS) -c -o $@ $<

run: all
	./$(TARGET)

run-zoom: all
	./$(TARGET) 1200 900

clean:
	rm -f $(OBJECTS) $(TARGET)
	@echo "Limpo."
