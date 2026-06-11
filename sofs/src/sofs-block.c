/*
 * sofs-block.c - Implementação da camada de blocos do sofs.
 *
 * Fornece init_block_layer / reset_block_layer e as funções auxiliares
 * read_block / write_block, que mapeiam números de bloco lógico em setores
 * físicos via a API read_sector / write_sector do apidisk.
 */

#include <stdlib.h>
#include <string.h>
#include "sofs-block.h"

/* -------------------------------------------------------------------------
 * Estado do módulo, definido por init_block_layer()
 * ---------------------------------------------------------------------- */

static unsigned int g_partition_first_sector = 0;
static unsigned int g_sectors_per_block = 0;
static int g_block_layer_ready = false;

/* -------------------------------------------------------------------------
 * Ciclo de vida da camada de blocos
 * ---------------------------------------------------------------------- */

int init_block_layer(unsigned int partition_first_sector,
                     unsigned int sectors_per_block)
{
    if (sectors_per_block == 0)
        return -1;

    g_partition_first_sector = partition_first_sector;
    g_sectors_per_block      = sectors_per_block;
    g_block_layer_ready      = true;
    return 0;
}

void reset_block_layer(void)
{
    g_partition_first_sector = 0;
    g_sectors_per_block      = 0;
    g_block_layer_ready      = false;
}

/* -------------------------------------------------------------------------
 * Funções auxiliares de E/S de blocos
 * ---------------------------------------------------------------------- */

int read_block(unsigned int block_num, unsigned char *buffer)
{
    unsigned int first_sector;
    unsigned int s;

    if (!g_block_layer_ready || buffer == NULL)
        return -1;

    first_sector = g_partition_first_sector + block_num * g_sectors_per_block;

    for (s = 0; s < g_sectors_per_block; s++) {
        if (read_sector(first_sector + s,
                        buffer + s * SECTOR_SIZE) != 0)
            return -1;
    }
    return 0;
}

int write_block(unsigned int block_num, unsigned char *buffer)
{
    unsigned int first_sector;
    unsigned int s;

    if (!g_block_layer_ready || buffer == NULL)
        return -1;

    first_sector = g_partition_first_sector + block_num * g_sectors_per_block;

    for (s = 0; s < g_sectors_per_block; s++) {
        if (write_sector(first_sector + s,
                         buffer + s * SECTOR_SIZE) != 0)
            return -1;
    }
    return 0;
}
