/*
 * sofs-block.h - Estruturas do layout do disco e API da camada de blocos do sofs.
 *
 * Este módulo define:
 *   - Tipos primitivos
 *   - Estruturas de dados em disco (MBR, superbloco, i-node, registro de diretório)
 *   - E/S de setores de baixo nível (via apidisk)
 *   - API de bitmap (via bitmap2)
 *   - Funções auxiliares de bloco (read_block / write_block)
 */

#ifndef SOFS_BLOCK_H
#define SOFS_BLOCK_H

/* -------------------------------------------------------------------------
 * Tipos primitivos
 * ---------------------------------------------------------------------- */

typedef int            boolean;
#define false          0
#define true           (!false)

typedef unsigned char       BYTE;
typedef unsigned short int  WORD;
typedef unsigned int        DWORD;

/* -------------------------------------------------------------------------
 * Constantes e API de E/S de setores (apidisk)
 * ---------------------------------------------------------------------- */

#define SECTOR_SIZE 256   /* todo setor tem exatamente 256 bytes */

/*
 * read_sector - lê um setor do disco virtual.
 *   sector : número lógico do setor (base 0)
 *   buffer : buffer de destino (deve ter pelo menos SECTOR_SIZE bytes)
 * Retorna 0 em caso de sucesso; valor diferente de zero em caso de erro.
 */
int read_sector(unsigned int sector, unsigned char *buffer);

/*
 * write_sector - grava um setor no disco virtual.
 *   sector : número lógico do setor (base 0)
 *   buffer : buffer de origem (deve ter pelo menos SECTOR_SIZE bytes)
 * Retorna 0 em caso de sucesso; valor diferente de zero em caso de erro.
 */
int write_sector(unsigned int sector, unsigned char *buffer);

/* -------------------------------------------------------------------------
 * Estruturas de dados em disco
 * ---------------------------------------------------------------------- */

#pragma pack(push, 1)

/* Entrada na tabela de partições do MBR */
struct sofs_partition_entry {
    char  name[16];        /* rótulo legível da partição (terminado em '\0') */
    DWORD reserved[2];     /* reservado, sempre 0 */
    DWORD firstSector;     /* primeiro setor lógico da partição */
    DWORD lastSector;      /* último setor lógico da partição (inclusive) */
};

/* Master Boot Record (setor 0 do disco virtual) */
struct sofs_mbr {
    WORD  version;         /* versão do sistema de arquivos: 0x7E32 */
    WORD  sectorSize;      /* tamanho do setor em bytes (sempre SECTOR_SIZE = 256) */
    WORD  blockSize;       /* setores por bloco padrão */
    WORD  numPartitions;   /* número máximo de partições na tabela */
    DWORD numValidParts;   /* número de partições formatadas/ativas */
    DWORD diskSize;        /* tamanho total do disco em setores */
    struct sofs_partition_entry partitionTable[4]; /* tabela de partições */
};

/* Registro de diretório (64 bytes): uma entrada por arquivo no diretório raiz */
#define TYPEVAL_INVALIDO  0x00
#define TYPEVAL_REGULAR   0x01
#define TYPEVAL_LINK      0x02

struct sofs_record {
    BYTE  TypeVal;         /* tipo de entrada: TYPEVAL_INVALIDO / REGULAR / LINK */
    char  name[51];        /* nome do arquivo (ASCII 0x21-0x7A), preenchido com '\0' */
    DWORD Nao_usado[2];    /* reservado para uso futuro */
    DWORD inodeNumber;     /* índice do i-node associado */
};

/* i-node (32 bytes): metadados de um arquivo ou link */
struct sofs_inode {
    DWORD blocksFileSize;  /* tamanho do arquivo em blocos (softlink: sempre 1) */
    DWORD bytesFileSize;   /* tamanho do arquivo em bytes */
    DWORD dataPtr[2];      /* dois ponteiros diretos de bloco */
    DWORD singleIndPtr;    /* ponteiro de indireção simples */
    DWORD doubleIndPtr;    /* ponteiro de indireção dupla */
    DWORD RefCounter;      /* contador de referências de hardlink */
    DWORD reservado;       /* reservado, deve ser 0 */
};

/* Superbloco (ocupa o primeiro bloco da partição) */
struct sofs_superbloco {
    char  id[4];                  /* "SOFS" */
    WORD  version;                /* 0x7E32 */
    WORD  superblockSize;         /* sempre 1 bloco */
    WORD  freeBlocksBitmapSize;   /* blocos usados pelo bitmap de blocos livres */
    WORD  freeInodeBitmapSize;    /* blocos usados pelo bitmap de i-nodes livres */
    WORD  inodeAreaSize;          /* blocos reservados para a área de i-nodes */
    WORD  blockSize;              /* setores por bloco nesta partição */
    DWORD diskSize;               /* total de blocos na partição */
    DWORD Checksum;               /* soma em complemento de um dos 5 primeiros DWORDs */
};

#pragma pack(pop)

/* -------------------------------------------------------------------------
 * API de bitmap (bitmap2) — fornecida como biblioteca pré-compilada
 * ---------------------------------------------------------------------- */

#define BITMAP_INODE  0
#define BITMAP_DADOS  1

/*
 * openBitmap2 - carrega em cache os bitmaps da partição montada.
 *   superbloco_sector : número absoluto de setor do superbloco.
 * Retorna 0 em caso de sucesso.
 */
int openBitmap2(int superbloco_sector);

/* closeBitmap2 - descarrega o cache no disco e libera a memória. Retorna 0. */
int closeBitmap2(void);

/*
 * getBitmap2 - lê um bit de um bitmap.
 *   handle    : BITMAP_INODE ou BITMAP_DADOS
 *   bitNumber : índice do bit a consultar
 * Retorna 0 ou 1 em caso de sucesso; valor negativo em caso de erro.
 */
int getBitmap2(int handle, int bitNumber);

/*
 * setBitmap2 - escreve um bit em um bitmap.
 *   handle    : BITMAP_INODE ou BITMAP_DADOS
 *   bitNumber : índice do bit a modificar
 *   bitValue  : 0 (livre) ou 1 (ocupado)
 * Retorna 0 em caso de sucesso; valor negativo em caso de erro.
 */
int setBitmap2(int handle, int bitNumber, int bitValue);

/*
 * searchBitmap2 - encontra o primeiro bit com o valor solicitado.
 *   handle   : BITMAP_INODE ou BITMAP_DADOS
 *   bitValue : valor a buscar (0 ou 1)
 * Retorna o índice do bit (>= 0) em caso de sucesso; 0 se não encontrado;
 * valor negativo em caso de erro.
 */
int searchBitmap2(int handle, int bitValue);

/* -------------------------------------------------------------------------
 * API da camada de blocos
 * ---------------------------------------------------------------------- */

/*
 * init_block_layer - inicializa a camada de blocos para uma partição.
 *   partition_first_sector : número absoluto do setor inicial da partição
 *   sectors_per_block      : número de setores que formam um bloco lógico
 *
 * Deve ser chamada antes de qualquer chamada a read_block / write_block.
 * As funções sofs_format() e sofs_mount() chamam esta função automaticamente.
 * Retorna 0 em caso de sucesso; valor diferente de zero em caso de erro.
 */
int init_block_layer(unsigned int partition_first_sector,
                     unsigned int sectors_per_block);

/* reset_block_layer - limpa o estado da camada de blocos (chamada por sofs_umount). */
void reset_block_layer(void);

/*
 * read_block - lê um bloco lógico da partição ativa.
 *   block_num : número do bloco na partição (0 = superbloco)
 *   buffer    : destino; deve ter pelo menos (sectors_per_block * SECTOR_SIZE) bytes
 * Retorna 0 em caso de sucesso; valor diferente de zero em caso de erro.
 */
int read_block(unsigned int block_num, unsigned char *buffer);

/*
 * write_block - grava um bloco lógico na partição ativa.
 *   block_num : número do bloco na partição (0 = superbloco)
 *   buffer    : dados de origem; deve ter exatamente (sectors_per_block * SECTOR_SIZE) bytes
 * Retorna 0 em caso de sucesso; valor diferente de zero em caso de erro.
 */
int write_block(unsigned int block_num, unsigned char *buffer);

#endif /* SOFS_BLOCK_H */
