/*
 * mandelbrot.h — Tipos compartilhados entre mandelbrot.c e ipc.c
 *
 * INF01142 — Sistemas Operacionais — 2026/1
 */

#ifndef MANDELBROT_H
#define MANDELBROT_H

/* =========================================================================
 * Parâmetros globais da renderização
 * ========================================================================= */
typedef struct {
    int    width;       /* largura do canvas em pixels        */
    int    height;      /* altura do canvas em pixels         */
    int    granularity; /* lado do tile em pixels             */
    int    max_iter;    /* máximo de iterações de Mandelbrot  */
    double x_min;       /* limite esquerdo no plano complexo  */
    double x_max;       /* limite direito no plano complexo   */
    double y_min;       /* limite inferior no plano complexo  */
    double y_max;       /* limite superior no plano complexo  */
} RenderParams;

/* =========================================================================
 * Descrição de um tile
 * ========================================================================= */
typedef struct {
    int ox; /* pixel x do canto superior esquerdo */
    int oy; /* pixel y do canto superior esquerdo */
    int w;  /* largura efetiva do tile em pixels  */
    int h;  /* altura efetiva do tile em pixels   */
} Tile;

/* =========================================================================
 * Resultado de um tile — preenchido pelo processo filho e lido pelo pai
 * ========================================================================= */
typedef struct {
    Tile           tile;    /* coordenadas e dimensões do tile         */
    unsigned char *pixels;  /* buffer w*h bytes, alocado pelo leitor   */
} TileResult;

/* =========================================================================
 * Função de cálculo — implementada em mandelbrot.c
 *
 * Chamada pelo filho (via worker_main em ipc.c) para calcular os pixels
 * de um tile e gravá-los no buffer buf (deve ter tile->w * tile->h bytes).
 * ========================================================================= */
void compute_tile(const RenderParams *params, const Tile *tile,
                  unsigned char *buf);

#endif /* MANDELBROT_H */
