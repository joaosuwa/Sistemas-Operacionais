/*
 * sofs.c - Implementação do sistema de arquivos sofs.
 *
 * A camada de blocos (sofs-block) é usada para todos os acessos ao disco;
 * a camada de bitmap (bitmap2) gerencia o controle de blocos e i-nodes livres.
 *
 * Layout do sistema de arquivos dentro de uma partição (em ordem):
 *   [bloco 0]          superbloco
 *   [blocos 1 .. bb]   bitmap de blocos livres   (bb = freeBlocksBitmapSize)
 *   [bb+1 .. bb+bi]    bitmap de i-nodes livres  (bi = freeInodeBitmapSize)
 *   [bb+bi+1 .. ...]   área de i-nodes           (10% dos blocos, arredondado para cima)
 *   [resto]            blocos de dados
 *
 * As funções auxiliares alloc_data_block(), free_data_block(),
 * alloc_inode() e free_inode() são fornecidas como blocos de construção.
 */

#include <string.h>
#include <stdlib.h>
#include "sofs.h"
#include "sofs-block.h"

#define MAX_OPEN_FILES 10

static struct open_file_entry {
    int used;
    unsigned int inode_num;
    int current_pos;
} g_open_files[MAX_OPEN_FILES];

static int g_dir_entry_index;

/* -------------------------------------------------------------------------
 * Estado interno de montagem
 * ---------------------------------------------------------------------- */

static int g_mounted = false;
static struct sofs_superbloco g_superbloco;
static unsigned int g_superbloco_sector;   /* setor absoluto do superbloco */

/* Forward declarations for functions defined later in the file */
static int alloc_data_block(void);
static int free_data_block(unsigned int abs_block_num);
static int alloc_inode(void);
static int free_inode(unsigned int inode_num);
static int block_lookup(struct sofs_inode *inode, int block_idx, int alloc);

/* -------------------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------------- */

static unsigned int block_size(void)
{
    return (unsigned int)g_superbloco.blockSize * SECTOR_SIZE;
}

static unsigned int ptrs_per_block(void)
{
    return block_size() / sizeof(DWORD);
}

static int read_inode(unsigned int inode_num, struct sofs_inode *inode)
{
    unsigned int inodes_per_block = block_size() / sizeof(struct sofs_inode);
    unsigned int inode_block = 1
        + g_superbloco.freeBlocksBitmapSize
        + g_superbloco.freeInodeBitmapSize
        + inode_num / inodes_per_block;
    unsigned int inode_offset = inode_num % inodes_per_block;
    unsigned char *buf = (unsigned char *)__builtin_alloca(block_size());

    if (read_block(inode_block, buf) != 0)
        return -1;

    memcpy(inode, buf + inode_offset * sizeof(struct sofs_inode),
           sizeof(struct sofs_inode));
    return 0;
}

static int write_inode(unsigned int inode_num, struct sofs_inode *inode)
{
    unsigned int inodes_per_block = block_size() / sizeof(struct sofs_inode);
    unsigned int inode_block = 1
        + g_superbloco.freeBlocksBitmapSize
        + g_superbloco.freeInodeBitmapSize
        + inode_num / inodes_per_block;
    unsigned int inode_offset = inode_num % inodes_per_block;
    unsigned char *buf = (unsigned char *)__builtin_alloca(block_size());

    if (read_block(inode_block, buf) != 0)
        return -1;

    memcpy(buf + inode_offset * sizeof(struct sofs_inode), inode,
           sizeof(struct sofs_inode));

    return write_block(inode_block, buf);
}

static int find_record(char *name, struct sofs_record *out_rec,
                       unsigned int *out_block, unsigned int *out_offset)
{
    struct sofs_inode root_inode;

    if (read_inode(0, &root_inode) != 0)
        return -1;

    unsigned int bs = block_size();
    unsigned int recs_per_block = bs / sizeof(struct sofs_record);
    unsigned char *buf = (unsigned char *)__builtin_alloca(bs);

    int block_idx = 0;
    while (1) {
        int phys_block = block_lookup(&root_inode, block_idx, 0);
        if (phys_block < 0)
            break;

        if (read_block((unsigned int)phys_block, buf) != 0)
            return -1;

        unsigned int r;
        for (r = 0; r < recs_per_block; r++) {
            struct sofs_record *rec = (struct sofs_record *)
                (buf + r * sizeof(struct sofs_record));
            if (rec->TypeVal != TYPEVAL_INVALIDO &&
                strncmp(rec->name, name, 50) == 0) {
                if (out_rec)   memcpy(out_rec, rec, sizeof(struct sofs_record));
                if (out_block) *out_block = (unsigned int)phys_block;
                if (out_offset)*out_offset = r;
                return (int)rec->inodeNumber;
            }
        }
        block_idx++;
    }

    return -1;
}

static int block_lookup(struct sofs_inode *inode, int block_idx, int alloc)
{
    unsigned int p_per_block = ptrs_per_block();

    if (block_idx < 0)
        return -1;

    if (block_idx < 2) {
        if (inode->dataPtr[block_idx] == 0) {
            if (!alloc) return -1;
            int blk = alloc_data_block();
            if (blk < 0) return -1;
            inode->dataPtr[block_idx] = (DWORD)blk;
        }
        return (int)inode->dataPtr[block_idx];
    }

    block_idx -= 2;

    if ((unsigned int)block_idx < p_per_block) {
        if (inode->singleIndPtr == 0) {
            if (!alloc) return -1;
            int blk = alloc_data_block();
            if (blk < 0) return -1;
            inode->singleIndPtr = (DWORD)blk;
        }
        unsigned char *ibuf = (unsigned char *)__builtin_alloca(block_size());
        if (read_block(inode->singleIndPtr, ibuf) != 0)
            return -1;
        DWORD *ptrs = (DWORD *)ibuf;
        if (ptrs[block_idx] == 0) {
            if (!alloc) return -1;
            int blk = alloc_data_block();
            if (blk < 0) return -1;
            ptrs[block_idx] = (DWORD)blk;
            if (write_block(inode->singleIndPtr, ibuf) != 0)
                return -1;
        }
        return (int)ptrs[block_idx];
    }

    block_idx -= (int)p_per_block;

    if ((unsigned int)block_idx < p_per_block * p_per_block) {
        unsigned int outer = (unsigned int)block_idx / p_per_block;
        unsigned int inner = (unsigned int)block_idx % p_per_block;

        if (inode->doubleIndPtr == 0) {
            if (!alloc) return -1;
            int blk = alloc_data_block();
            if (blk < 0) return -1;
            inode->doubleIndPtr = (DWORD)blk;
        }

        unsigned char *dbuf = (unsigned char *)__builtin_alloca(block_size());
        if (read_block(inode->doubleIndPtr, dbuf) != 0)
            return -1;
        DWORD *dptrs = (DWORD *)dbuf;

        if (dptrs[outer] == 0) {
            if (!alloc) return -1;
            int blk = alloc_data_block();
            if (blk < 0) return -1;
            dptrs[outer] = (DWORD)blk;
            if (write_block(inode->doubleIndPtr, dbuf) != 0)
                return -1;
        }

        unsigned char *ibuf = (unsigned char *)__builtin_alloca(block_size());
        if (read_block(dptrs[outer], ibuf) != 0)
            return -1;
        DWORD *ptrs = (DWORD *)ibuf;
        if (ptrs[inner] == 0) {
            if (!alloc) return -1;
            int blk = alloc_data_block();
            if (blk < 0) return -1;
            ptrs[inner] = (DWORD)blk;
            if (write_block(dptrs[outer], ibuf) != 0)
                return -1;
        }
        return (int)ptrs[inner];
    }

    return -1;
}

static int free_inode_blocks(struct sofs_inode *inode)
{
    unsigned int p_per_block = ptrs_per_block();

    int i;
    for (i = 0; i < 2; i++) {
        if (inode->dataPtr[i] != 0) {
            free_data_block(inode->dataPtr[i]);
            inode->dataPtr[i] = 0;
        }
    }

    if (inode->singleIndPtr != 0) {
        unsigned char *ibuf = (unsigned char *)__builtin_alloca(block_size());
        if (read_block(inode->singleIndPtr, ibuf) == 0) {
            DWORD *ptrs = (DWORD *)ibuf;
            unsigned int j;
            for (j = 0; j < p_per_block; j++) {
                if (ptrs[j] != 0)
                    free_data_block(ptrs[j]);
            }
        }
        free_data_block(inode->singleIndPtr);
        inode->singleIndPtr = 0;
    }

    if (inode->doubleIndPtr != 0) {
        unsigned char *dbuf = (unsigned char *)__builtin_alloca(block_size());
        if (read_block(inode->doubleIndPtr, dbuf) == 0) {
            DWORD *dptrs = (DWORD *)dbuf;
            unsigned int j;
            for (j = 0; j < p_per_block; j++) {
                if (dptrs[j] != 0) {
                    unsigned char *ibuf2 = (unsigned char *)__builtin_alloca(block_size());
                    if (read_block(dptrs[j], ibuf2) == 0) {
                        DWORD *ptrs = (DWORD *)ibuf2;
                        unsigned int k;
                        for (k = 0; k < p_per_block; k++) {
                            if (ptrs[k] != 0)
                                free_data_block(ptrs[k]);
                        }
                    }
                    free_data_block(dptrs[j]);
                }
            }
        }
        free_data_block(inode->doubleIndPtr);
        inode->doubleIndPtr = 0;
    }

    return 0;
}

static int add_dir_record(char *name, BYTE type, unsigned int inode_num)
{
    struct sofs_inode root_inode;
    if (read_inode(0, &root_inode) != 0)
        return -1;

    unsigned int bs = block_size();
    unsigned int recs_per_block = bs / sizeof(struct sofs_record);
    unsigned char *buf = (unsigned char *)__builtin_alloca(bs);

    int block_idx = 0;
    while (1) {
        int phys_block = block_lookup(&root_inode, block_idx, 1);
        if (phys_block < 0)
            return -1;

        if (read_block((unsigned int)phys_block, buf) != 0)
            return -1;

        unsigned int r;
        for (r = 0; r < recs_per_block; r++) {
            struct sofs_record *rec = (struct sofs_record *)
                (buf + r * sizeof(struct sofs_record));
            if (rec->TypeVal == TYPEVAL_INVALIDO) {
                memset(rec, 0, sizeof(struct sofs_record));
                rec->TypeVal = type;
                strncpy(rec->name, name, 50);
                rec->inodeNumber = (DWORD)inode_num;
                if (write_block((unsigned int)phys_block, buf) != 0)
                    return -1;
                /* Persiste alterações no i-node raiz (blocos podem ter sido alocados) */
                return write_inode(0, &root_inode);
            }
        }
        block_idx++;
    }
}

static int alloc_open_file(unsigned int inode_num)
{
    int i;
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        if (!g_open_files[i].used) {
            g_open_files[i].used = 1;
            g_open_files[i].inode_num = inode_num;
            g_open_files[i].current_pos = 0;
            return i;
        }
    }
    return -1;
}

/* -------------------------------------------------------------------------
 * Auxiliar: lê o MBR e localiza a partição <partition>.
 * Preenche *first_sector e *num_sectors.
 * Retorna 0 em caso de sucesso.
 * ---------------------------------------------------------------------- */
static int read_partition_info(int partition,
                               unsigned int *first_sector,
                               unsigned int *num_sectors)
{
    unsigned char mbr_buf[SECTOR_SIZE];
    struct sofs_mbr *mbr;

    if (read_sector(0, mbr_buf) != 0)
        return -1;

    mbr = (struct sofs_mbr *)mbr_buf;

    if (partition < 0 || partition >= (int)mbr->numPartitions)
        return -1;

    *first_sector = mbr->partitionTable[partition].firstSector;
    *num_sectors  = mbr->partitionTable[partition].lastSector
                    - mbr->partitionTable[partition].firstSector + 1;
    return 0;
}

/* -------------------------------------------------------------------------
 * Funções básicas de criação/destruição de blocos de dados e i-nodes.
 *
 * Fornecidas como blocos de construção para a implementação do grupo em
 * sofs_create, sofs_delete, sofs_read, sofs_write, etc.
 * ---------------------------------------------------------------------- */

/*
 * alloc_data_block - aloca o primeiro bloco de dados livre.
 *
 * Pesquisa no bitmap de dados o primeiro bit livre, marca-o como ocupado,
 * zera o conteúdo do bloco e retorna o número absoluto do bloco na partição.
 *
 * Retorna o número do bloco (>= 0) em caso de sucesso; -1 em caso de erro
 * ou se o disco estiver cheio.
 */
static int alloc_data_block(void)
{
    int bit;
    unsigned int block_size;
    unsigned char *buf;

    if (!g_mounted)
        return -1;

    bit = searchBitmap2(BITMAP_DADOS, 0);
    if (bit < 0)
        return -1;

    if (setBitmap2(BITMAP_DADOS, bit, 1) != 0)
        return -1;

    /* Inicializa o bloco recém-alocado com zeros */
    block_size = g_superbloco.blockSize * SECTOR_SIZE;
    buf = (unsigned char *)__builtin_alloca(block_size);
    memset(buf, 0, block_size);

    /* O primeiro bloco de dados começa após superbloco + bitmaps + área de i-nodes */
    unsigned int first_data_block = 1
        + g_superbloco.freeBlocksBitmapSize
        + g_superbloco.freeInodeBitmapSize
        + g_superbloco.inodeAreaSize;

    if (write_block(first_data_block + (unsigned int)bit, buf) != 0) {
        setBitmap2(BITMAP_DADOS, bit, 0);
        return -1;
    }

    return (int)(first_data_block + (unsigned int)bit);
}

/*
 * free_data_block - libera um bloco de dados previamente alocado.
 *
 *   abs_block_num : número absoluto do bloco na partição (conforme
 *                   retornado por alloc_data_block).
 *
 * Retorna 0 em caso de sucesso; -1 em caso de erro.
 */
static int free_data_block(unsigned int abs_block_num)
{
    unsigned int first_data_block;
    int bit;

    if (!g_mounted)
        return -1;

    first_data_block = 1
        + g_superbloco.freeBlocksBitmapSize
        + g_superbloco.freeInodeBitmapSize
        + g_superbloco.inodeAreaSize;

    if (abs_block_num < first_data_block)
        return -1;

    bit = (int)(abs_block_num - first_data_block);
    return setBitmap2(BITMAP_DADOS, bit, 0);
}

/*
 * alloc_inode - aloca o primeiro i-node livre.
 *
 * Pesquisa no bitmap de i-nodes o primeiro bit livre, marca-o como ocupado,
 * zera o conteúdo do i-node em disco e retorna o número do i-node.
 *
 * Retorna o número do i-node (>= 0) em caso de sucesso; -1 em caso de erro
 * ou se todos os i-nodes estiverem em uso.
 */
static int alloc_inode(void)
{
    int bit;
    unsigned int inode_block;
    unsigned int inodes_per_block;
    unsigned int inode_offset;
    unsigned char *buf;
    unsigned int block_size;

    if (!g_mounted)
        return -1;

    bit = searchBitmap2(BITMAP_INODE, 0);
    if (bit < 0)
        return -1;

    if (setBitmap2(BITMAP_INODE, bit, 1) != 0)
        return -1;

    /* Zera o i-node em disco */
    block_size     = g_superbloco.blockSize * SECTOR_SIZE;
    inodes_per_block = block_size / sizeof(struct sofs_inode);
    inode_block    = 1
        + g_superbloco.freeBlocksBitmapSize
        + g_superbloco.freeInodeBitmapSize
        + (unsigned int)bit / inodes_per_block;
    inode_offset   = (unsigned int)bit % inodes_per_block;

    buf = (unsigned char *)__builtin_alloca(block_size);
    if (read_block(inode_block, buf) != 0) {
        setBitmap2(BITMAP_INODE, bit, 0);
        return -1;
    }

    memset(buf + inode_offset * sizeof(struct sofs_inode), 0,
           sizeof(struct sofs_inode));

    if (write_block(inode_block, buf) != 0) {
        setBitmap2(BITMAP_INODE, bit, 0);
        return -1;
    }

    return bit;
}

/*
 * free_inode - libera um i-node previamente alocado.
 *
 *   inode_num : número do i-node (conforme retornado por alloc_inode).
 *
 * Retorna 0 em caso de sucesso; -1 em caso de erro.
 */
static int free_inode(unsigned int inode_num)
{
    if (!g_mounted)
        return -1;

    return setBitmap2(BITMAP_INODE, (int)inode_num, 0);
}

/* -------------------------------------------------------------------------
 * Gerência do sistema de arquivos
 * ---------------------------------------------------------------------- */

int sofs_identify(char *name, int size)
{
    const char *id = "sofs group";
    if (name == NULL || size <= 0)
        return -1;
    strncpy(name, id, size - 1);
    name[size - 1] = '\0';
    return 0;
}

int sofs_format(int partition, int sectors_per_block)
{
    unsigned int first_sector, num_sectors;
    unsigned int num_blocks;
    unsigned int inode_area_blocks;
    unsigned int bitmap_blocks_data;
    unsigned int bitmap_blocks_inode;
    unsigned char block_buf[sectors_per_block * SECTOR_SIZE];
    struct sofs_superbloco *sb;

    if (sectors_per_block <= 0)
        return -1;

    if (read_partition_info(partition, &first_sector, &num_sectors) != 0)
        return -1;

    /* Inicializa a camada de blocos para poder escrever na partição */
    if (init_block_layer(first_sector, (unsigned int)sectors_per_block) != 0)
        return -1;

    num_blocks = num_sectors / (unsigned int)sectors_per_block;

    /* 10% dos blocos para i-nodes, arredondado para cima */
    inode_area_blocks = (num_blocks + 9) / 10;

    /* Um bloco por 8*(sectors_per_block*SECTOR_SIZE) bits necessários em cada bitmap */
    bitmap_blocks_data  = (num_blocks + 8 * sectors_per_block * SECTOR_SIZE - 1)
                          / (8 * sectors_per_block * SECTOR_SIZE);
    bitmap_blocks_inode = (inode_area_blocks + 8 * sectors_per_block * SECTOR_SIZE - 1)
                          / (8 * sectors_per_block * SECTOR_SIZE);

    /* Constrói e grava o superbloco (bloco 0 da partição) */
    memset(block_buf, 0, sizeof(block_buf));
    sb = (struct sofs_superbloco *)block_buf;
    memcpy(sb->id, "SOFS", 4);
    sb->version              = 0x7E32;
    sb->superblockSize       = 1;
    sb->freeBlocksBitmapSize = (WORD)bitmap_blocks_data;
    sb->freeInodeBitmapSize  = (WORD)bitmap_blocks_inode;
    sb->inodeAreaSize        = (WORD)inode_area_blocks;
    sb->blockSize            = (WORD)sectors_per_block;
    sb->diskSize             = (DWORD)num_blocks;

    /* Checksum: complemento de um da soma dos 5 primeiros DWORDs */
    {
        DWORD *words = (DWORD *)block_buf;
        DWORD  sum   = words[0] + words[1] + words[2] + words[3] + words[4];
        sb->Checksum = ~sum;
    }

    if (write_block(0, block_buf) != 0)
        return -1;

    /* Inicializa com zeros as áreas de bitmap e de i-nodes */
    memset(block_buf, 0, sizeof(block_buf));
    {
        unsigned int blk;
        for (blk = 1; blk < 1 + (unsigned int)bitmap_blocks_data +
             (unsigned int)bitmap_blocks_inode + inode_area_blocks; blk++) {
            if (write_block(blk, block_buf) != 0)
                return -1;
        }
    }

    /* Marca i-node 0 (diretório raiz) como ocupado no bitmap de i-nodes:
     * o bitmap de i-nodes começa no bloco 1 + freeBlocksBitmapSize.
     * Cada byte = 8 bits; bit 0 do byte 0 = i-node 0. */
    {
        unsigned char bitbuf[SECTOR_SIZE];
        unsigned int bitmap_inode_block = 1 + (unsigned int)bitmap_blocks_data;
        unsigned int sector_offset = 0; /* i-node 0 está no bit 0 => byte 0 do primeiro setor */
        /* Usamos write_sector diretamente pois o bitmap2 ainda não foi aberto.
         * Calculamos o setor absoluto para escrever. */
        {
            unsigned int abs_bitmap_sector = first_sector
                + bitmap_inode_block * (unsigned int)sectors_per_block
                + sector_offset / SECTOR_SIZE;
            if (read_sector(abs_bitmap_sector, bitbuf) != 0)
                return -1;
            bitbuf[0] |= 0x01; /* seta bit 0 */
            if (write_sector(abs_bitmap_sector, bitbuf) != 0)
                return -1;
        }
    }

    return 0;
}

int sofs_mount(int partition)
{
    unsigned int first_sector, num_sectors;
    unsigned char sector_buf[SECTOR_SIZE];
    struct sofs_superbloco *sb;

    if (g_mounted)
        return -1;  /* partição já montada */

    if (read_partition_info(partition, &first_sector, &num_sectors) != 0)
        return -1;

    /* Lê o primeiro setor da partição para obter o superbloco */
    if (read_sector(first_sector, sector_buf) != 0)
        return -1;

    sb = (struct sofs_superbloco *)sector_buf;

    /* Valida a assinatura do sistema de arquivos */
    if (memcmp(sb->id, "SOFS", 4) != 0)
        return -1;

    /* Agora sabemos o tamanho do bloco: inicializa a camada de blocos */
    if (init_block_layer(first_sector, (unsigned int)sb->blockSize) != 0)
        return -1;

    /* Abre o subsistema de bitmap */
    g_superbloco_sector = first_sector;
    if (openBitmap2((int)g_superbloco_sector) != 0)
        return -1;

    /* Armazena em cache o superbloco */
    memcpy(&g_superbloco, sb, sizeof(g_superbloco));
    g_mounted = true;
    return 0;
}

int sofs_umount(void)
{
    if (!g_mounted)
        return -1;

    closeBitmap2();
    reset_block_layer();
    memset(&g_superbloco, 0, sizeof(g_superbloco));
    g_mounted = false;
    return 0;
}

/* -------------------------------------------------------------------------
 * Operações de arquivo (TODO)
 * ---------------------------------------------------------------------- */

SOFS_FILE sofs_create(char *filename)
{
    unsigned int inode_num;
    struct sofs_inode inode;
    struct sofs_record rec;
    int existing;
    int handle;

    if (!g_mounted || filename == NULL || strlen(filename) == 0)
        return -1;

    /* Verifica se há espaço na tabela antes de criar */
    {
        int i;
        int free_slot = -1;
        for (i = 0; i < MAX_OPEN_FILES; i++) {
            if (!g_open_files[i].used) { free_slot = i; break; }
        }
        if (free_slot < 0)
            return -1;
    }

    existing = find_record(filename, &rec, NULL, NULL);

    if (existing >= 0) {
        inode_num = (unsigned int)existing;
        if (read_inode(inode_num, &inode) != 0)
            return -1;
        free_inode_blocks(&inode);
        inode.blocksFileSize = 0;
        inode.bytesFileSize = 0;
        if (write_inode(inode_num, &inode) != 0)
            return -1;
    } else {
        int new_inode = alloc_inode();
        if (new_inode < 0)
            return -1;
        inode_num = (unsigned int)new_inode;

        memset(&inode, 0, sizeof(inode));
        if (write_inode(inode_num, &inode) != 0) {
            free_inode(inode_num);
            return -1;
        }

        if (add_dir_record(filename, TYPEVAL_REGULAR, inode_num) != 0) {
            free_inode(inode_num);
            return -1;
        }
    }

    handle = alloc_open_file(inode_num);
    /* alloc_open_file não falha pois já verificamos acima */
    return (SOFS_FILE)handle;
}

int sofs_delete(char *name)
{
    struct sofs_record rec;
    unsigned int rec_block, rec_offset;
    struct sofs_inode inode;
    unsigned char *buf;
    unsigned int bs;

    if (!g_mounted || name == NULL)
        return -1;

    int inode_num = find_record(name, &rec, &rec_block, &rec_offset);
    if (inode_num < 0)
        return -1;

    if (read_inode((unsigned int)inode_num, &inode) != 0)
        return -1;

    free_inode_blocks(&inode);

    free_inode((unsigned int)inode_num);

    /* Invalida o registro de diretório */
    bs = block_size();
    buf = (unsigned char *)__builtin_alloca(bs);
    if (read_block(rec_block, buf) != 0)
        return -1;

    struct sofs_record *r = (struct sofs_record *)
        (buf + rec_offset * sizeof(struct sofs_record));
    memset(r, 0, sizeof(struct sofs_record));
    r->TypeVal = TYPEVAL_INVALIDO;

    return write_block(rec_block, buf);
}

SOFS_FILE sofs_open(char *name)
{
    struct sofs_record rec;
    unsigned int inode_num;

    if (!g_mounted || name == NULL)
        return -1;

    int res = find_record(name, &rec, NULL, NULL);
    if (res < 0)
        return -1;

    inode_num = (unsigned int)res;

    /* Se for softlink, resolve */
    if (rec.TypeVal == TYPEVAL_LINK) {
        struct sofs_inode link_inode;
        unsigned int bs = block_size();
        unsigned char *buf = (unsigned char *)__builtin_alloca(bs);

        if (read_inode(inode_num, &link_inode) != 0)
            return -1;
        if (link_inode.dataPtr[0] == 0)
            return -1;
        if (read_block(link_inode.dataPtr[0], buf) != 0)
            return -1;

        /* Re-abre o arquivo alvo (recursão simples, sem proteção contra ciclos) */
        return sofs_open((char *)buf);
    }

    return (SOFS_FILE)alloc_open_file(inode_num);
}

int sofs_close(SOFS_FILE handle)
{
    if (handle < 0 || handle >= MAX_OPEN_FILES)
        return -1;
    if (!g_open_files[handle].used)
        return -1;

    g_open_files[handle].used = 0;
    return 0;
}

int sofs_read(SOFS_FILE handle, char *buffer, int size)
{
    struct sofs_inode inode;
    unsigned int bs;
    unsigned char *buf;
    int file_size;
    int to_read;
    int pos;

    if (handle < 0 || handle >= MAX_OPEN_FILES || !g_open_files[handle].used)
        return -1;
    if (buffer == NULL || size <= 0)
        return 0;

    if (read_inode(g_open_files[handle].inode_num, &inode) != 0)
        return -1;

    pos = g_open_files[handle].current_pos;
    file_size = (int)inode.bytesFileSize;

    if (pos >= file_size)
        return 0;

    to_read = size;
    if (pos + to_read > file_size)
        to_read = file_size - pos;

    bs = block_size();
    buf = (unsigned char *)__builtin_alloca(bs);
    int bytes_done = 0;

    while (bytes_done < to_read) {
        int current_byte = pos + bytes_done;
        int block_idx = current_byte / (int)bs;
        int block_off = current_byte % (int)bs;

        int phys = block_lookup(&inode, block_idx, 0);
        if (phys < 0)
            break;

        if (read_block((unsigned int)phys, buf) != 0)
            break;

        int chunk = to_read - bytes_done;
        if (chunk > (int)bs - block_off)
            chunk = (int)bs - block_off;

        memcpy(buffer + bytes_done, buf + block_off, chunk);
        bytes_done += chunk;
    }

    g_open_files[handle].current_pos += bytes_done;
    return bytes_done;
}

int sofs_write(SOFS_FILE handle, char *buffer, int size)
{
    struct sofs_inode inode;
    unsigned int bs;
    unsigned char *buf;
    int pos;
    int bytes_done;

    if (handle < 0 || handle >= MAX_OPEN_FILES || !g_open_files[handle].used)
        return -1;
    if (buffer == NULL || size <= 0)
        return 0;

    if (read_inode(g_open_files[handle].inode_num, &inode) != 0)
        return -1;

    pos = g_open_files[handle].current_pos;
    bs = block_size();
    buf = (unsigned char *)__builtin_alloca(bs);
    bytes_done = 0;

    while (bytes_done < size) {
        int current_byte = pos + bytes_done;
        int block_idx = current_byte / (int)bs;
        int block_off = current_byte % (int)bs;

        int phys = block_lookup(&inode, block_idx, 1);
        if (phys < 0)
            break;

        if (read_block((unsigned int)phys, buf) != 0)
            break;

        int chunk = size - bytes_done;
        if (chunk > (int)bs - block_off)
            chunk = (int)bs - block_off;

        memcpy(buf + block_off, buffer + bytes_done, chunk);

        if (write_block((unsigned int)phys, buf) != 0)
            break;

        bytes_done += chunk;
    }

    /* Atualiza o tamanho do arquivo se necessário */
    if (pos + bytes_done > (int)inode.bytesFileSize) {
        inode.bytesFileSize = (DWORD)(pos + bytes_done);
        /* Recalcula blocksFileSize */
        inode.blocksFileSize = (inode.bytesFileSize + bs - 1) / bs;
        write_inode(g_open_files[handle].inode_num, &inode);
    }

    g_open_files[handle].current_pos += bytes_done;
    return bytes_done;
}

/* -------------------------------------------------------------------------
 * Operações de diretório (TODO)
 * ---------------------------------------------------------------------- */

int sofs_opendir(void)
{
    if (!g_mounted)
        return -1;

    g_dir_entry_index = 0;
    return 0;
}

int sofs_readdir(SOFS_DIRENT *dentry)
{
    struct sofs_inode root_inode;
    unsigned int bs;
    unsigned char *buf;

    if (!g_mounted || dentry == NULL)
        return -1;

    if (read_inode(0, &root_inode) != 0)
        return -1;

    bs = block_size();
    unsigned int recs_per_block = bs / sizeof(struct sofs_record);
    buf = (unsigned char *)__builtin_alloca(bs);

    while (1) {
        int block_idx = g_dir_entry_index / (int)recs_per_block;
        int rec_off   = g_dir_entry_index % (int)recs_per_block;

        int phys = block_lookup(&root_inode, block_idx, 0);
        if (phys < 0) {
            /* Não há mais blocos de dados no diretório raiz */
            return 1;
        }

        if (read_block((unsigned int)phys, buf) != 0)
            return -1;

        unsigned int r;
        for (r = (unsigned int)rec_off; r < recs_per_block; r++) {
            struct sofs_record *rec = (struct sofs_record *)
                (buf + r * sizeof(struct sofs_record));
            g_dir_entry_index++;

            if (rec->TypeVal != TYPEVAL_INVALIDO) {
                struct sofs_inode file_inode;
                memset(dentry->name, 0, sizeof(dentry->name));
                strncpy(dentry->name, rec->name, SOFS_MAX_FILE_NAME_SIZE);
                dentry->fileType = rec->TypeVal;
                dentry->fileSize = 0;

                if (read_inode(rec->inodeNumber, &file_inode) == 0)
                    dentry->fileSize = file_inode.bytesFileSize;

                return 0;
            }
        }
    }
}

int sofs_closedir(void)
{
    g_dir_entry_index = 0;
    return 0;
}

/* -------------------------------------------------------------------------
 * Operações de link (TODO)
 * ---------------------------------------------------------------------- */

int sofs_sln(char *linkname, char *filename)
{
    unsigned int bs;
    unsigned char *buf;
    struct sofs_inode inode;
    int new_inode;
    int data_block;

    if (!g_mounted || linkname == NULL || filename == NULL)
        return -1;

    new_inode = alloc_inode();
    if (new_inode < 0)
        return -1;

    data_block = alloc_data_block();
    if (data_block < 0) {
        free_inode((unsigned int)new_inode);
        return -1;
    }

    /* Escreve o nome do alvo no bloco de dados */
    bs = block_size();
    buf = (unsigned char *)__builtin_alloca(bs);
    memset(buf, 0, bs);
    strncpy((char *)buf, filename, bs - 1);

    if (write_block((unsigned int)data_block, buf) != 0) {
        free_data_block((unsigned int)data_block);
        free_inode((unsigned int)new_inode);
        return -1;
    }

    /* Configura o i-node do link */
    memset(&inode, 0, sizeof(inode));
    inode.dataPtr[0] = (DWORD)data_block;
    inode.blocksFileSize = 1;
    inode.bytesFileSize = (DWORD)strlen(filename);

    if (write_inode((unsigned int)new_inode, &inode) != 0) {
        free_data_block((unsigned int)data_block);
        free_inode((unsigned int)new_inode);
        return -1;
    }

    if (add_dir_record(linkname, TYPEVAL_LINK, (unsigned int)new_inode) != 0) {
        free_data_block((unsigned int)data_block);
        free_inode((unsigned int)new_inode);
        return -1;
    }

    return 0;
}

int sofs_hln(char *linkname, char *filename)
{
    struct sofs_record rec;
    struct sofs_inode inode;

    if (!g_mounted || linkname == NULL || filename == NULL)
        return -1;

    int inode_num = find_record(filename, &rec, NULL, NULL);
    if (inode_num < 0)
        return -1;

    if (read_inode((unsigned int)inode_num, &inode) != 0)
        return -1;

    inode.RefCounter++;

    if (write_inode((unsigned int)inode_num, &inode) != 0)
        return -1;

    if (add_dir_record(linkname, TYPEVAL_REGULAR, (unsigned int)inode_num) != 0) {
        inode.RefCounter--;
        write_inode((unsigned int)inode_num, &inode);
        return -1;
    }

    return 0;
}
