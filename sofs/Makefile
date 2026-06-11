#
# Makefile para libsofs.a e exemplo de uso
#
# Inspirado no "A Simple Makefile Tutorial"
# (https://www.cs.colby.edu/maxwell/courses/tutorials/maketutor/)
#
# Alvos:
#   all      - constrói lib/libsofs.a (padrão)
#   exemplo  - compila e linka o programa de exemplo (bin/exemplo)
#   check    - compila um smoke-test mínimo contra libsofs.a
#   clean    - remove objetos gerados e a biblioteca
#

CC      = gcc
CFLAGS  = -Wall -Wextra -std=c99 -I$(INC_DIR) -m32
# NOTA: os objetos pré-compilados (apidisk.o, bitmap2.o) são 32 bits (i386).
# Adicione -m32 ao CFLAGS e compile normalmente.
# Em host 64 bits com gcc-multilib, também use -m32.

INC_DIR = ./include
SRC_DIR = ./src
BIN_DIR = ./bin
LIB_DIR = ./lib

# Arquivos-fonte que compilamos nós mesmos
SRCS = $(SRC_DIR)/sofs-block.c \
       $(SRC_DIR)/sofs.c

# Objetos correspondentes (gerados em BIN_DIR)
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BIN_DIR)/%.o)

# Objetos pré-compilados do fornecedor (não removidos pelo clean)
VENDOR_OBJS = $(LIB_DIR)/apidisk.o \
              $(LIB_DIR)/bitmap2.o

LIBRARY = $(LIB_DIR)/libsofs.a
EXEMPLO = $(BIN_DIR)/exemplo

# --------------------------------------------------------------------------
# Alvo padrão
# --------------------------------------------------------------------------
all: $(LIBRARY)

$(LIBRARY): $(OBJS) $(VENDOR_OBJS)
	ar rcs $@ $(OBJS) $(VENDOR_OBJS)

# Regra padrão: compila cada .c em SRC_DIR para um .o em BIN_DIR
$(BIN_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# --------------------------------------------------------------------------
# Exemplo: compila e linka o programa de demonstração
#
# ATENÇÃO: requer ambiente com gcc-multilib + -m32,
# pois os objetos pré-compilados (apidisk.o, bitmap2.o) são i386.
# O executável bin/exemplo deve ser executado no mesmo diretório que
# contém o arquivo t2fs_disk.dat (necessário para o apidisk).
# --------------------------------------------------------------------------
exemplo: $(LIBRARY) $(SRC_DIR)/exemplo.c
	$(CC) $(CFLAGS) -m32 $(SRC_DIR)/exemplo.c -o $(EXEMPLO) -L$(LIB_DIR) -lsofs

# --------------------------------------------------------------------------
# Smoke-test: compila um programa mínimo contra os cabeçalhos da libsofs.a
# para verificar que a API é auto-consistente.
# --------------------------------------------------------------------------
CHECK_SRC = $(BIN_DIR)/check_link.c

check:
	@printf '#include "sofs.h"\nint main(void){char b[64];sofs_identify(b,sizeof(b));return 0;}\n' \
		> $(CHECK_SRC)
	$(CC) $(CFLAGS) -c $(CHECK_SRC) -o /dev/null
	@echo "Smoke-test de compilação: OK"
	@rm -f $(CHECK_SRC)

# --------------------------------------------------------------------------
# Clean: remove objetos gerados e a biblioteca; objetos do fornecedor são mantidos
# --------------------------------------------------------------------------
clean:
	rm -f $(OBJS) $(LIBRARY) $(EXEMPLO)
	rm -f $(BIN_DIR)/*~ $(SRC_DIR)/*~ $(INC_DIR)/*~ *~

.PHONY: all exemplo check clean
