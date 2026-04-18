/*
 * ipc.h — Interface do módulo de IPC (fork + pipes)
 *
 * INF01142 — Sistemas Operacionais — 2026/1
 *
 * Este cabeçalho define a interface que deve ser implementada
 * em ipc.c. Não modifique este arquivo.
 *
 * Estrutura opaca Pool
 * --------------------
 * Pool é um tipo opaco que mantém o estado do pool de processos filhos:
 * PIDs ativos, descritores dos pipes de retorno e contagem de filhos.
 * Sua definição interna fica em ipc.c — o código externo acessa o pool
 * apenas pelas funções abaixo.
 */

#ifndef IPC_H
#define IPC_H

#include "mandelbrot.h"

/* Tipo opaco — definido internamente em ipc.c */
typedef struct Pool Pool;

/* =========================================================================
 * pool_create — aloca e inicializa um pool para no máximo max_children
 * filhos simultâneos.
 *
 * Retorna ponteiro para o pool alocado, ou NULL em caso de erro.
 * ========================================================================= */
Pool *pool_create(int max_children);

/* =========================================================================
 * pool_destroy — libera todos os recursos do pool (memória, descritores).
 *
 * Deve ser chamado após todos os filhos terem sido coletados.
 * ========================================================================= */
void pool_destroy(Pool *pool);

/* =========================================================================
 * pool_active — retorna o número de filhos atualmente ativos no pool.
 * ========================================================================= */
int pool_active(const Pool *pool);

/* =========================================================================
 * launch_worker — lança um processo filho para calcular o tile *t.
 *
 * Deve:
 *   1. Criar um pipe de retorno: pipe(fd) → fd[0]=leitura, fd[1]=escrita.
 *   2. Chamar fork().
 *   3. No filho:
 *        - Fechar fd[0] (o filho não lê do pipe de retorno).
 *        - Chamar worker_main(params, t, fd[1]).
 *        - O worker_main não retorna; o filho termina dentro dele.
 *   4. No pai:
 *        - Fechar fd[1] (o pai não escreve no pipe de retorno).
 *        - Registrar pid e fd[0] no pool.
 *
 * Dica: se o fork() falhar, reportar com perror() e retornar sem travar.
 * ========================================================================= */
void launch_worker(Pool *pool, const RenderParams *params, const Tile *t);

/* =========================================================================
 * worker_main — código executado pelo processo filho.
 *
 * Deve:
 *   1. Alocar um buffer de tile->w * tile->h bytes.
 *   2. Chamar compute_tile(params, tile, buf) para calcular os pixels.
 *   3. Escrever no pipe write_fd o seguinte protocolo (sem padding):
 *
 *        [ tile.ox : int           (4 bytes) ]
 *        [ tile.oy : int           (4 bytes) ]
 *        [ tile.w  : int           (4 bytes) ]
 *        [ tile.h  : int           (4 bytes) ]
 *        [ pixels  : unsigned char (w*h bytes) ]
 *
 *      Lembre-se: write() pode transferir menos bytes que o pedido —
 *      escreva em loop até enviar todos os bytes.
 *   4. Fechar write_fd.
 *   5. Liberar o buffer.
 *   6. Chamar exit(0).
 *
 * Esta função nunca retorna.
 * ========================================================================= */
void worker_main(const RenderParams *params, const Tile *tile, int write_fd);

/* =========================================================================
 * pool_collect_ready — lê um resultado de qualquer filho que já tenha
 * dados disponíveis no pipe, SEM BLOQUEAR.
 *
 * Deve usar select() (ou poll()) com timeout zero para descobrir quais
 * pipes têm dados. Se houver dados em algum pipe:
 *   1. Ler o cabeçalho (ox, oy, w, h — 4 ints).
 *   2. Alocar result->pixels com malloc(w * h).
 *   3. Ler os w*h bytes de pixels (em loop, pois read() pode ser parcial).
 *   4. Preencher result->tile e result->pixels.
 *   5. Retornar 1.
 *
 * Se nenhum pipe tiver dados, retornar 0 sem bloquear.
 *
 * O chamador é responsável por liberar result->pixels com free().
 * ========================================================================= */
int pool_collect_ready(Pool *pool, TileResult *result);

/* =========================================================================
 * pool_reap — coleta filhos que já terminaram, sem bloquear.
 *
 * Deve chamar waitpid(-1, &status, WNOHANG) em loop até não haver mais
 * filhos encerrados a coletar. Para cada filho coletado, fechar o pipe
 * correspondente (se ainda aberto) e liberar a entrada no pool.
 * ========================================================================= */
void pool_reap(Pool *pool);

#endif /* IPC_H */
