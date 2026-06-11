/*
 * sofs.h - API pública da biblioteca do sistema de arquivos sofs (libsofs.a).
 *
 * Implementa a especificação sofs:
 *   diretório de nível único, arquivos regulares, softlinks e hardlinks.
 *
 * Todas as funções retornam 0 (ou um handle não-negativo) em caso de sucesso
 * e um valor negativo em caso de erro, salvo indicação contrária.
 */

#ifndef SOFS_H
#define SOFS_H

#include "sofs-block.h"   /* BYTE, WORD, DWORD */

/* -------------------------------------------------------------------------
 * Tipos
 * ---------------------------------------------------------------------- */

typedef int SOFS_FILE;   /* handle de arquivo retornado por sofs_create / sofs_open */

/* Informações de entrada de diretório retornadas por sofs_readdir */
#define SOFS_MAX_FILE_NAME_SIZE 50

#pragma pack(push, 1)
typedef struct {
    char  name[SOFS_MAX_FILE_NAME_SIZE + 1]; /* nome do arquivo (terminado em '\0') */
    BYTE  fileType;                          /* TYPEVAL_REGULAR (0x01) ou TYPEVAL_LINK (0x02) */
    DWORD fileSize;                          /* tamanho do arquivo em bytes */
} SOFS_DIRENT;
#pragma pack(pop)

/* -------------------------------------------------------------------------
 * Gerência do sistema de arquivos
 * ---------------------------------------------------------------------- */

/*
 * sofs_identify - copia a string de identificação dos desenvolvedores em *name.
 *   name : buffer de destino
 *   size : número máximo de bytes a copiar (incluindo '\0')
 * Retorna 0 em caso de sucesso.
 */
int sofs_identify(char *name, int size);

/*
 * sofs_format - formata logicamente a partição <partition> para o sistema de arquivos sofs.
 *   partition        : número da partição (índice na tabela de partições do MBR)
 *   sectors_per_block: número de setores por bloco lógico
 *
 * Cria o superbloco, o bitmap de blocos livres, o bitmap de i-nodes livres,
 * a área de i-nodes e o diretório raiz.  10% dos blocos são reservados para
 * i-nodes (arredondado para cima).
 * Retorna 0 em caso de sucesso.
 */
int sofs_format(int partition, int sectors_per_block);

/*
 * sofs_mount - monta a partição <partition> no ponto de montagem raiz "/".
 *   partition : número da partição a montar
 * Retorna 0 em caso de sucesso.
 */
int sofs_mount(int partition);

/*
 * sofs_umount - desmonta a partição atualmente montada e libera "/".
 * Retorna 0 em caso de sucesso.
 */
int sofs_umount(void);

/* -------------------------------------------------------------------------
 * Operações de arquivo
 * ---------------------------------------------------------------------- */

/*
 * sofs_create - cria um novo arquivo e o abre.
 *   filename : nome do novo arquivo (até 50 caracteres alfanuméricos)
 *
 * Se o arquivo já existir, seu conteúdo é truncado para zero bytes.
 * O ponteiro de posição corrente é colocado em 0.
 * Retorna um handle não-negativo em caso de sucesso; negativo em caso de erro.
 */
SOFS_FILE sofs_create(char *filename);

/*
 * sofs_delete - remove um arquivo ou link do sistema de arquivos.
 *   name : nome do arquivo ou link a remover
 * Retorna 0 em caso de sucesso.
 */
int sofs_delete(char *name);

/*
 * sofs_open - abre um arquivo ou link existente para leitura e escrita.
 *   name : nome do arquivo ou link a abrir
 *
 * O ponteiro de posição corrente é colocado em 0.
 * Retorna um handle não-negativo em caso de sucesso; negativo em caso de erro.
 */
SOFS_FILE sofs_open(char *name);

/*
 * sofs_close - fecha o arquivo identificado por <handle>.
 *   handle : handle retornado por sofs_create ou sofs_open
 * Retorna 0 em caso de sucesso.
 */
int sofs_close(SOFS_FILE handle);

/*
 * sofs_read - lê até <size> bytes do arquivo para <buffer>.
 *   handle : handle de arquivo aberto
 *   buffer : buffer de destino
 *   size   : número de bytes a ler
 *
 * Lê a partir do ponteiro de posição corrente; avança o ponteiro após a leitura.
 * Retorna o número de bytes efetivamente lidos (pode ser menor que <size> no
 * fim do arquivo), ou valor negativo em caso de erro.
 */
int sofs_read(SOFS_FILE handle, char *buffer, int size);

/*
 * sofs_write - grava <size> bytes de <buffer> no arquivo.
 *   handle : handle de arquivo aberto
 *   buffer : buffer de origem
 *   size   : número de bytes a gravar
 *
 * Grava a partir do ponteiro de posição corrente; avança o ponteiro após a escrita.
 * Retorna o número de bytes gravados, ou valor negativo em caso de erro.
 */
int sofs_write(SOFS_FILE handle, char *buffer, int size);

/* -------------------------------------------------------------------------
 * Operações de diretório
 * ---------------------------------------------------------------------- */

/*
 * sofs_opendir - abre o diretório raiz para leitura sequencial.
 *
 * Posiciona o ponteiro de entradas na primeira entrada válida.
 * Retorna 0 em caso de sucesso.
 */
int sofs_opendir(void);

/*
 * sofs_readdir - lê a próxima entrada válida do diretório em *dentry.
 *   dentry : estrutura de saída a preencher
 *
 * Avança o ponteiro de entradas para a próxima entrada válida.
 * Retorna 0 em caso de sucesso; valor diferente de zero quando não há mais
 * entradas válidas ou em caso de erro.
 */
int sofs_readdir(SOFS_DIRENT *dentry);

/*
 * sofs_closedir - fecha o diretório raiz.
 * Retorna 0 em caso de sucesso.
 */
int sofs_closedir(void);

/* -------------------------------------------------------------------------
 * Operações de link
 * ---------------------------------------------------------------------- */

/*
 * sofs_sln - cria um link simbólico (softlink).
 *   linkname : nome do novo link
 *   filename : nome do arquivo alvo
 * Retorna 0 em caso de sucesso.
 */
int sofs_sln(char *linkname, char *filename);

/*
 * sofs_hln - cria um link estrito (hardlink).
 *   linkname : nome do novo link
 *   filename : nome do arquivo existente a vincular
 * Retorna 0 em caso de sucesso.
 */
int sofs_hln(char *linkname, char *filename);

#endif /* SOFS_H */
