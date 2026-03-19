/*
 * colors.h — Interface da paleta de cores para o fractal de Mandelbrot
 *
 * INF01142 — Sistemas Operacionais — 2026/1
 *
 * Adaptado de "Fractal @ PCAD" (GPL v3).
 */

#ifndef COLORS_H
#define COLORS_H

#include <raylib.h>

/* Número de modos de paleta disponíveis em get_current_pallette_color (0..8) */
#define TOTAL_COLORS 9

/* Tamanho da paleta Viridis (pontos-chave para interpolação linear) */
#define VIRIDIS_SIZE 16

/* Tipos de função usados internamente em colors.c */
typedef double (*normalize_fn)(int depth, int max_depth);
typedef Color  (*color_fn)(double t);

/* Paleta Viridis — definida em colors.c */
extern Color viridis_palette[VIRIDIS_SIZE];

/*
 * get_current_pallette_color — mapeia (depth, max_depth) para uma cor
 * usando o modo de paleta selecionado por current_color (0..TOTAL_COLORS-1).
 *
 * depth == max_depth → preto (ponto pertence ao conjunto).
 */
Color get_current_pallette_color(int current_color, int depth, int max_depth);

#endif /* COLORS_H */
