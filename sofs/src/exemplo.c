/*
 * exemplo.c - Exemplo de uso do sistema de arquivos sofs.
 *
 * Este programa demonstra como utilizar as funções já implementadas
 * do sofs: sofs_format, sofs_mount e sofs_umount.
 *
 * As seções comentadas mostram como testar as funções que o grupo
 * deverá implementar: criação e leitura de arquivos, listagem do
 * diretório raiz e criação de links.
 *
 * Compilação (via Makefile):
 *   make exemplo
 *
 * Execução (o arquivo t2fs_disk.dat deve estar no diretório corrente):
 *   ./bin/exemplo
 */

#include <stdio.h>
#include <string.h>
#include "sofs.h"

/* -------------------------------------------------------------------------
 * Utilitário: lista todas as entradas válidas do diretório raiz.
 *
 * Chama sofs_opendir / sofs_readdir / sofs_closedir.
 * Esta função só funcionará após a implementação do grupo.
 * ---------------------------------------------------------------------- */
static void listar_diretorio(void)
{
    SOFS_DIRENT entrada;
    int ret;

    printf("=== Conteúdo do diretório raiz ===\n");

    if (sofs_opendir() != 0) {
        printf("  [erro] sofs_opendir falhou\n");
        return;
    }

    while ((ret = sofs_readdir(&entrada)) == 0) {
        printf("  %-50s  tipo=%s  tamanho=%u bytes\n",
               entrada.name,
               entrada.fileType == TYPEVAL_REGULAR ? "regular" : "link",
               (unsigned int)entrada.fileSize);
    }

    sofs_closedir();
    printf("=== Fim do diretório ===\n\n");
}

int main(void)
{
    int particao = 0;       /* número da partição a formatar/montar */
    int setores_por_bloco = 2; /* tamanho de bloco = 2 * 256 = 512 bytes */

    /* ------------------------------------------------------------------
     * 1. Identificação
     * ------------------------------------------------------------------ */
    char identificacao[64];
    sofs_identify(identificacao, sizeof(identificacao));
    printf("Identificação: %s\n\n", identificacao);

    /* ------------------------------------------------------------------
     * 2. Formatar a partição
     *
     * sofs_format cria o superbloco, os bitmaps e a área de i-nodes.
     * Deve ser chamado uma única vez; após isso, use sofs_mount.
     * ------------------------------------------------------------------ */
    printf("Formatando partição %d com %d setores/bloco...\n",
           particao, setores_por_bloco);

    if (sofs_format(particao, setores_por_bloco) != 0) {
        fprintf(stderr, "ERRO: sofs_format falhou.\n"
                "Verifique se t2fs_disk.dat está no diretório corrente.\n");
        return 1;
    }
    printf("sofs_format: OK\n\n");

    /* ------------------------------------------------------------------
     * 3. Montar a partição
     * ------------------------------------------------------------------ */
    printf("Montando partição %d...\n", particao);

    if (sofs_mount(particao) != 0) {
        fprintf(stderr, "ERRO: sofs_mount falhou.\n");
        return 1;
    }
    printf("sofs_mount: OK\n\n");

    /* ------------------------------------------------------------------
     * 4. Listar o diretório raiz (recém-formatado, deve estar vazio)
     *
     * ATENÇÃO: esta chamada só funciona após implementar sofs_opendir,
     * sofs_readdir e sofs_closedir.
     * ------------------------------------------------------------------ */
    listar_diretorio();

    /* ------------------------------------------------------------------
     * 5. Criar arquivos  [DESCOMENTE após implementar sofs_create/sofs_write]
     * ------------------------------------------------------------------
    {
        SOFS_FILE arq;
        char dados[] = "Olá, sofs!";

        printf("Criando arquivo 'teste.txt'...\n");
        arq = sofs_create("teste.txt");
        if (arq < 0) {
            printf("  [erro] sofs_create falhou (esperado antes da implementação)\n");
        } else {
            int n = sofs_write(arq, dados, (int)strlen(dados));
            printf("  sofs_write: %d bytes gravados\n", n);
            sofs_close(arq);
        }

        printf("Criando arquivo 'dados.bin'...\n");
        arq = sofs_create("dados.bin");
        if (arq >= 0) {
            char buf[256];
            memset(buf, 0xAB, sizeof(buf));
            sofs_write(arq, buf, sizeof(buf));
            sofs_close(arq);
        }
    }
    * ------------------------------------------------------------------ */

    /* ------------------------------------------------------------------
     * 6. Ler um arquivo  [DESCOMENTE após implementar sofs_open/sofs_read]
     * ------------------------------------------------------------------
    {
        SOFS_FILE arq;
        char buf[128];
        int n;

        printf("Lendo arquivo 'teste.txt'...\n");
        arq = sofs_open("teste.txt");
        if (arq < 0) {
            printf("  [erro] sofs_open falhou\n");
        } else {
            memset(buf, 0, sizeof(buf));
            n = sofs_read(arq, buf, sizeof(buf) - 1);
            printf("  sofs_read: %d bytes lidos: \"%s\"\n", n, buf);
            sofs_close(arq);
        }
    }
    * ------------------------------------------------------------------ */

    /* ------------------------------------------------------------------
     * 7. Listar diretório após criação dos arquivos
     *    [DESCOMENTE junto com a seção 5]
     * ------------------------------------------------------------------
    listar_diretorio();
    * ------------------------------------------------------------------ */

    /* ------------------------------------------------------------------
     * 8. Criar softlink e hardlink  [DESCOMENTE após implementar sofs_sln/sofs_hln]
     * ------------------------------------------------------------------
    printf("Criando softlink 'link_soft' -> 'teste.txt'...\n");
    if (sofs_sln("link_soft", "teste.txt") != 0)
        printf("  [erro] sofs_sln falhou\n");
    else
        printf("  sofs_sln: OK\n");

    printf("Criando hardlink 'link_hard' -> 'dados.bin'...\n");
    if (sofs_hln("link_hard", "dados.bin") != 0)
        printf("  [erro] sofs_hln falhou\n");
    else
        printf("  sofs_hln: OK\n");

    listar_diretorio();
    * ------------------------------------------------------------------ */

    /* ------------------------------------------------------------------
     * 9. Remover arquivo  [DESCOMENTE após implementar sofs_delete]
     * ------------------------------------------------------------------
    printf("Removendo arquivo 'teste.txt'...\n");
    if (sofs_delete("teste.txt") != 0)
        printf("  [erro] sofs_delete falhou\n");
    else
        printf("  sofs_delete: OK\n");

    listar_diretorio();
    * ------------------------------------------------------------------ */

    /* ------------------------------------------------------------------
     * 10. Desmontar a partição
     * ------------------------------------------------------------------ */
    printf("Desmontando partição...\n");

    if (sofs_umount() != 0) {
        fprintf(stderr, "ERRO: sofs_umount falhou.\n");
        return 1;
    }
    printf("sofs_umount: OK\n\n");

    printf("Exemplo concluído.\n");
    return 0;
}
