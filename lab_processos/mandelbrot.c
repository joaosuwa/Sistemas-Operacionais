/*
 * mandelbrot.c — Renderizador interativo e paralelo do fractal de Mandelbrot
 *
 * INF01142 — Sistemas Operacionais — 2026/1
 *
 * Este arquivo contém a infraestrutura completa do programa:
 *   - parsing de argumentos
 *   - cálculo do Mandelbrot (função compute_tile)
 *   - renderização interativa com RayLib (janela, paletas, seleção de área)
 *   - loop principal de gerenciamento do pool
 *
 * DEVE-SE IMPLEMENTAR as funções marcadas com TODO em ipc.c.
 * Não é necessário modificar este arquivo.
 *
 * Uso:
 *   ./mandelbrot [largura [altura]]
 *   (padrões: largura=800, altura=600)
 *
 * Interatividade:
 *   Enter          — ativar box de seleção / confirmar zoom
 *   Backspace      — cancelar seleção
 *   Ctrl+Z         — desfazer último zoom
 *   Space          — ciclar paleta de cores (sem re-renderizar)
 *   G                — aumentar profundidade de iterações (aplicada no próximo zoom)
 *   F                — diminuir profundidade de iterações (aplicada no próximo zoom)
 *   T                — aumentar granularidade (aplicada no próximo zoom)
 *   R                — diminuir granularidade (aplicada no próximo zoom)
 *   B                — aumentar número máximo de filhos (aplicado no próximo zoom)
 *   V                — diminuir número máximo de filhos, mín. 1 (aplicado no próximo zoom)
 *   WASD / setas   — mover box de seleção
 *   Shift+WASD     — redimensionar box de seleção
 *   Roda do mouse  — zoom da box de seleção
 *   Arraste (LMB)  — mover box de seleção com o mouse
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <math.h>
#include <sys/wait.h>

#include "raylib.h"
#include "mandelbrot.h"
#include "ipc.h"
#include "colors.h"

/* =========================================================================
 * Cálculo do fractal
 * ========================================================================= */

void compute_tile(const RenderParams *params, const Tile *tile,
                  unsigned char *buf)
{
    for (int py = 0; py < tile->h; py++) {
        for (int px = 0; px < tile->w; px++) {

            double cx = params->x_min +
                        (tile->ox + px) * (params->x_max - params->x_min)
                        / params->width;
            double cy = params->y_min +
                        (tile->oy + py) * (params->y_max - params->y_min)
                        / params->height;

            double zx = 0.0, zy = 0.0;
            int iter = 0;
            while (zx * zx + zy * zy <= 4.0 && iter < params->max_iter) {
                double tmp = zx * zx - zy * zy + cx;
                zy = 2.0 * zx * zy + cy;
                zx = tmp;
                iter++;
            }

            buf[py * tile->w + px] = (iter == params->max_iter)
                                     ? 0
                                     : (unsigned char)(iter * 255 / params->max_iter);
        }
    }
}

/* =========================================================================
 * Geração da lista de tiles
 * ========================================================================= */

static Tile *generate_tiles(const RenderParams *params, int *n_tiles)
{
    int G  = params->granularity;
    int nx = (params->width  + G - 1) / G;
    int ny = (params->height + G - 1) / G;
    *n_tiles = nx * ny;

    Tile *tiles = malloc(sizeof(Tile) * (size_t)(*n_tiles));
    if (!tiles) { perror("malloc tiles"); exit(1); }

    int idx = 0;
    for (int ty = 0; ty < ny; ty++) {
        for (int tx = 0; tx < nx; tx++) {
            tiles[idx].ox = tx * G;
            tiles[idx].oy = ty * G;
            tiles[idx].w  = (tiles[idx].ox + G <= params->width)
                            ? G : (params->width  - tiles[idx].ox);
            tiles[idx].h  = (tiles[idx].oy + G <= params->height)
                            ? G : (params->height - tiles[idx].oy);
            idx++;
        }
    }
    return tiles;
}

/* =========================================================================
 * GUI — constantes e estado
 * ========================================================================= */

#define GUI_MAX_HISTORY  64
#define GUI_MIN_GRAN      1
#define GUI_MAX_GRAN    500
#define GUI_MIN_ITER      2
#define GUI_MAX_ITER  65536   /* 256*256 */

/* Efeito de brilho nas tiles ao serem recebidas */
#define GLOW_MAX   512     /* máximo de glows simultâneos      */
#define GLOW_LIFE  0.55f   /* duração do efeito em segundos    */
#define GLOW_LAYERS  4     /* número de camadas concêntricas   */

typedef struct {
    Rectangle rect;
    float     timer;   /* tempo restante; <= 0 = inativo */
} TileGlow;

static TileGlow g_glows[GLOW_MAX];

/* Paleta e seleção */
static int       g_current_color      = 0;
static float     g_granularity        = 20.0f;
static float     g_depth_f            = 1000.0f;
static bool      g_selecting          = false;
static Rectangle g_box                = {0.0f, 0.0f, 0.0f, 0.0f};

/* HUD de ajuste de parâmetros */
static float     g_show_changes_timer = 0.0f;
static bool      g_gchanged           = false;
static bool      g_dchanged           = false;
static bool      g_mchanged           = false;  /* max_children alterado */

/* Número máximo de filhos simultâneos (GUI — aplicado no próximo zoom) */
static int       g_max_children       = 8;

/* Buffer de pixels: valores brutos (0-255) e cores correntes */
static unsigned char *g_pixel_vals    = NULL;
static Color         *g_pixel_colors  = NULL;
static bool           g_pixels_changed = false;

/* Histórico de regiões para desfazer */
static RenderParams g_history[GUI_MAX_HISTORY];
static int          g_history_count = 0;

/* Pedido de re-renderização diferida */
static bool         g_restart_pending = false;
static RenderParams g_pending_params;

/* =========================================================================
 * pixel_to_color — mapeia valor 0-255 do tile para uma cor da paleta atual
 * ========================================================================= */
static Color pixel_to_color(unsigned char val)
{
    if (val == 0) return (Color){0, 0, 0, 255};          /* pertence ao conjunto */
    return get_current_pallette_color(g_current_color, (int)val, 256);
}

/* =========================================================================
 * recolor_all — reaplica a paleta atual a todos os pixels já calculados,
 *               sem precisar re-renderizar com os processos filhos.
 * ========================================================================= */
static void recolor_all(int n_pixels)
{
    for (int i = 0; i < n_pixels; i++)
        g_pixel_colors[i] = pixel_to_color(g_pixel_vals[i]);
    g_pixels_changed = true;
}

/* =========================================================================
 * draw_tile_result — grava resultado de um tile nos buffers de pixel
 * ========================================================================= */
static void draw_tile_result(const TileResult *result, int screen_width)
{
    for (int py = 0; py < result->tile.h; py++) {
        for (int px = 0; px < result->tile.w; px++) {
            unsigned char val = result->pixels[py * result->tile.w + px];
            int idx = (result->tile.oy + py) * screen_width
                    + (result->tile.ox + px);
            g_pixel_vals[idx]   = val;
            g_pixel_colors[idx] = pixel_to_color(val);
        }
    }
    g_pixels_changed = true;

    /* Registrar brilho para este tile */
    Rectangle r = {
        (float)result->tile.ox, (float)result->tile.oy,
        (float)result->tile.w,  (float)result->tile.h
    };
    /* Procura slot livre; se não houver, recicla o mais antigo */
    int slot = 0;
    for (int k = 0; k < GLOW_MAX; k++) {
        if (g_glows[k].timer <= 0.0f) { slot = k; break; }
        if (g_glows[k].timer < g_glows[slot].timer) slot = k;
    }
    g_glows[slot].rect  = r;
    g_glows[slot].timer = GLOW_LIFE;
}

/* =========================================================================
 * draw_glows — atualiza e desenha os efeitos de brilho nas tiles recebidas.
 * Deve ser chamado entre BeginDrawing() e EndDrawing(), após DrawTexture().
 * ========================================================================= */
static void draw_glows(float dt)
{
    for (int k = 0; k < GLOW_MAX; k++) {
        if (g_glows[k].timer <= 0.0f) continue;

        g_glows[k].timer -= dt;
        float t = g_glows[k].timer / GLOW_LIFE;   /* 1 = recém-recebido, 0 = apagado */
        if (t < 0.0f) t = 0.0f;

        /* Curva de easing: brilho rápido no início, suave no fim */
        float intensity = t * t;

        for (int layer = 0; layer < GLOW_LAYERS; layer++) {
            float expand = (float)layer * 2.0f;
            Rectangle r = {
                g_glows[k].rect.x      - expand,
                g_glows[k].rect.y      - expand,
                g_glows[k].rect.width  + expand * 2.0f,
                g_glows[k].rect.height + expand * 2.0f
            };

            /* Camadas externas mais transparentes */
            float layer_fade = 1.0f - (float)layer / (float)GLOW_LAYERS;
            unsigned char alpha = (unsigned char)(220.0f * intensity * layer_fade);

            /* Cor: branco-amarelo quando recente, laranja-vermelho ao apagar */
            Color c = {
                255,
                (unsigned char)(230.0f * t),
                (unsigned char)( 80.0f * t),
                alpha
            };
            DrawRectangleLinesEx(r, layer == 0 ? 1.5f : 1.0f, c);
        }
    }
}

/* =========================================================================
 * handle_input — trata teclado e mouse; retorna true se apenas uma
 *                recoloração (sem restart) é necessária.
 *
 * Quando o usuário confirma uma seleção ou desfaz, seta g_restart_pending
 * e g_pending_params para que o loop principal reinicie a renderização.
 * ========================================================================= */
static bool handle_input(const RenderParams *cur)
{
    bool need_recolor = false;
    float dt = GetFrameTime();
    float sw = (float)cur->width;
    float sh = (float)cur->height;
    float move_speed = IsKeyDown(KEY_LEFT_CONTROL) ? sw / 10.0f : sw / 3.0f;

    /* --- ENTER: ativar box / confirmar zoom -------------------------------- */
    if (IsKeyPressed(KEY_ENTER)) {
        if (g_selecting) {
            g_selecting = false;
            /* Confirma apenas se o box tem tamanho mínimo */
            if (g_box.width >= 4.0f && g_box.height >= 4.0f) {
                double xr = (cur->x_max - cur->x_min) / cur->width;
                double yr = (cur->y_max - cur->y_min) / cur->height;

                RenderParams np = *cur;
                np.granularity = (int)g_granularity;
                np.max_iter    = (int)g_depth_f;
                np.x_min = cur->x_min + (double)g_box.x               * xr;
                np.x_max = cur->x_min + (double)(g_box.x + g_box.width)  * xr;
                np.y_min = cur->y_min + (double)g_box.y               * yr;
                np.y_max = cur->y_min + (double)(g_box.y + g_box.height) * yr;

                if (g_history_count < GUI_MAX_HISTORY)
                    g_history[g_history_count++] = *cur;

                g_pending_params  = np;
                g_restart_pending = true;
            }
        } else {
            /* Inicializa box no centro (50% da tela) */
            g_selecting = true;
            g_box = (Rectangle){sw / 4.0f, sh / 4.0f, sw / 2.0f, sh / 2.0f};
        }
    }

    /* --- BACKSPACE: cancelar seleção sem zoom ------------------------------ */
    if (IsKeyPressed(KEY_BACKSPACE))
        g_selecting = false;

    /* --- Ctrl+Z: desfazer último zoom -------------------------------------- */
    if (IsKeyPressed(KEY_Z) && IsKeyDown(KEY_LEFT_CONTROL)) {
        if (g_history_count > 0) {
            g_history_count--;
            g_pending_params  = g_history[g_history_count];
            g_restart_pending = true;
            g_selecting       = false;
        }
    }

    /* --- F: diminuir profundidade / G: aumentar profundidade -------------- */
    {
        float change = g_depth_f * dt;
        if (IsKeyDown(KEY_F)) {
            g_depth_f -= change;
            g_dchanged = true;
            g_show_changes_timer = 1.0f;
        }
        if (IsKeyDown(KEY_G)) {
            g_depth_f += change;
            g_dchanged = true;
            g_show_changes_timer = 1.0f;
        }
        if (g_depth_f < (float)GUI_MIN_ITER) g_depth_f = (float)GUI_MIN_ITER;
        if (g_depth_f > (float)GUI_MAX_ITER) g_depth_f = (float)GUI_MAX_ITER;
    }

    /* --- R: diminuir granularidade / T: aumentar granularidade ------------ */
    {
        float change = g_granularity * dt;
        if (IsKeyDown(KEY_R)) {
            g_granularity -= change;
            g_gchanged = true;
            g_show_changes_timer = 1.0f;
        }
        if (IsKeyDown(KEY_T)) {
            g_granularity += change;
            g_gchanged = true;
            g_show_changes_timer = 1.0f;
        }
        if (g_granularity < (float)GUI_MIN_GRAN) g_granularity = (float)GUI_MIN_GRAN;
        if (g_granularity > (float)GUI_MAX_GRAN) g_granularity = (float)GUI_MAX_GRAN;
    }

    /* --- SPACE: ciclar paleta de cores (recolore sem re-renderizar) -------- */
    if (IsKeyPressed(KEY_SPACE)) {
        g_current_color = (g_current_color + 1) % TOTAL_COLORS;
        need_recolor = true;
    }

    /* --- V: diminuir filhos / B: aumentar filhos -------------------------- */
    if (IsKeyPressed(KEY_V)) {
        if (g_max_children > 1) {
            g_max_children--;
            g_mchanged = true;
            g_show_changes_timer = 1.0f;
        }
    }
    if (IsKeyPressed(KEY_B)) {
        g_max_children++;
        g_mchanged = true;
        g_show_changes_timer = 1.0f;
    }

    /* --- Interação com o box de seleção ------------------------------------ */
    if (g_selecting) {
        /* Arrastar o box com o botão esquerdo do mouse */
        Vector2 mouse = GetMousePosition();
        if (mouse.x > g_box.x && mouse.y > g_box.y &&
            mouse.x < g_box.x + g_box.width &&
            mouse.y < g_box.y + g_box.height &&
            IsMouseButtonDown(MOUSE_BUTTON_LEFT)) {
            Vector2 delta = GetMouseDelta();
            g_box.x += delta.x;
            g_box.y += delta.y;
        }

        /* Zoom com a roda do mouse */
        float wheel = GetMouseWheelMove();
        if (wheel != 0.0f) {
            float z = dt * wheel * 3.0f;
            float nw = g_box.width  - z * sw;
            float nh = g_box.height - z * sh;
            if (nw >= 4.0f && nh >= 4.0f) {
                g_box.x      += z * sw / 2.0f;
                g_box.y      += z * sh / 2.0f;
                g_box.width   = nw;
                g_box.height  = nh;
            }
        }

        /* Mover box com teclado (sem Shift) */
        if (!IsKeyDown(KEY_LEFT_SHIFT)) {
            if (IsKeyDown(KEY_UP)    || IsKeyDown(KEY_W)) g_box.y -= move_speed * dt;
            if (IsKeyDown(KEY_DOWN)  || IsKeyDown(KEY_S)) g_box.y += move_speed * dt;
            if (IsKeyDown(KEY_LEFT)  || IsKeyDown(KEY_A)) g_box.x -= move_speed * dt;
            if (IsKeyDown(KEY_RIGHT) || IsKeyDown(KEY_D)) g_box.x += move_speed * dt;
        }

        /* Redimensionar box com Shift + teclado */
        if (IsKeyDown(KEY_LEFT_SHIFT)) {
            float step = IsKeyDown(KEY_LEFT_CONTROL) ? dt / 10.0f : dt;
            int zdir = 0;
            if (IsKeyDown(KEY_UP)   || IsKeyDown(KEY_W)) zdir =  1;
            if (IsKeyDown(KEY_DOWN) || IsKeyDown(KEY_S)) zdir = -1;
            float z = step * (float)zdir;
            float nw = g_box.width  + z * sw;
            float nh = g_box.height + z * sh;
            if (nw >= 4.0f && nh >= 4.0f) {
                g_box.x      -= z * sw / 2.0f;
                g_box.y      -= z * sh / 2.0f;
                g_box.width   = nw;
                g_box.height  = nh;
            }
        }

        /* Limitar o box à tela (margem de 25% do próprio box) */
        float mx = g_box.width  / 4.0f;
        float my = g_box.height / 4.0f;
        if (g_box.x < -mx)           g_box.x = -mx;
        if (g_box.y < -my)           g_box.y = -my;
        if (g_box.x > sw - g_box.width  + mx) g_box.x = sw - g_box.width  + mx;
        if (g_box.y > sh - g_box.height + my) g_box.y = sh - g_box.height + my;
    }

    /* --- Decaimento do HUD de parâmetros ---------------------------------- */
    if (g_show_changes_timer > 0.0f) {
        g_show_changes_timer -= dt;
        if (g_show_changes_timer <= 0.0f) {
            g_show_changes_timer = 0.0f;
            g_gchanged = false;
            g_dchanged = false;
            g_mchanged = false;
        }
    }

    return need_recolor;
}

/* =========================================================================
 * Loop principal
 * ========================================================================= */

int main(int argc, char *argv[])
{
    /* --- Parsing de argumentos ---
     * Uso: ./mandelbrot [largura [altura]]
     * Padrões: cx=-0.7269, cy=0.1889, zoom=50.0, largura=800, altura=600 */
    if (argc > 3) {
        fprintf(stderr,
            "Uso: %s [largura [altura]]\n"
            "Exemplo: %s\n"
            "         %s 1200 900\n",
            argv[0], argv[0], argv[0]);
        return 1;
    }

    RenderParams params;
    double cx_center   = -0.7269;
    double cy_center   =  0.1889;
    double zoom        = 50.0;
    params.width       = 800;
    params.height      = 600;
    params.granularity = 20;
    params.max_iter    = 1000;

    if (argc >= 2) params.width  = atoi(argv[1]);
    if (argc >= 3) params.height = atoi(argv[2]);

    if (params.width <= 0 || params.height <= 0) {
        fprintf(stderr, "Erro: largura e altura devem ser positivos.\n");
        return 1;
    }

    double aspect = (double)params.width / params.height;
    double half_h = 1.2 / zoom;
    double half_w = half_h * aspect;
    params.x_min = cx_center - half_w;
    params.x_max = cx_center + half_w;
    params.y_min = cy_center - half_h;
    params.y_max = cy_center + half_h;

    /* Sincroniza sliders da GUI com os parâmetros iniciais */
    g_granularity = (float)params.granularity;
    g_depth_f     = (float)params.max_iter;

    /* --- Buffers de pixel (um por pixel da janela) --- */
    int n_pixels = params.width * params.height;
    g_pixel_vals   = calloc((size_t)n_pixels, sizeof(unsigned char));
    g_pixel_colors = malloc((size_t)n_pixels * sizeof(Color));
    if (!g_pixel_vals || !g_pixel_colors) {
        perror("malloc pixel buffers");
        return 1;
    }
    for (int i = 0; i < n_pixels; i++)
        g_pixel_colors[i] = (Color){0, 0, 0, 255};

    /* --- Inicialização da janela RayLib --- */
    SetTraceLogLevel(LOG_WARNING);
    InitWindow(params.width, params.height,
               "Mandelbrot — INF01142 Sistemas Operacionais");
    SetTargetFPS(60);

    /* Textura para renderização via UpdateTexture (evita DrawPixel por tile) */
    Image img = GenImageColor(params.width, params.height, BLACK);
    Texture2D texture = LoadTextureFromImage(img);
    UnloadImage(img);

    /* --- Inicialização do pool de IPC --- */
    Pool *pool = pool_create(g_max_children);
    if (!pool) { fprintf(stderr, "Erro ao criar pool.\n"); return 1; }

    /* --- Tiles iniciais --- */
    int   n_tiles;
    Tile *tiles     = generate_tiles(&params, &n_tiles);
    int   next_tile  = 0;
    int   done_tiles = 0;
    bool  rendering  = true;

    /* -----------------------------------------------------------------------
     * Loop principal
     *
     * Cada iteração:
     *   1. Trata input do usuário.
     *   2. Realiza restart (nova região) se pendente e pool ocioso.
     *   3. Recolore pixels se a paleta mudou (sem restart).
     *   4. Lança filhos enquanto houver tiles e slots livres no pool.
     *   5. Coleta resultados prontos e grava nos buffers.
     *   6. Coleta filhos encerrados com WNOHANG.
     *   7. Atualiza textura e desenha frame.
     * --------------------------------------------------------------------- */
    while (!WindowShouldClose()) {

        /* 1. Input */
        bool need_recolor = handle_input(&params);

        /* 2. Restart diferido: só executa quando pool está ocioso */
        if (g_restart_pending && pool_active(pool) == 0) {
            g_restart_pending = false;
            params = g_pending_params;

            /* Resincroniza sliders com os novos parâmetros */
            g_granularity = (float)params.granularity;
            g_depth_f     = (float)params.max_iter;

            /* Recria pool com o tamanho de filhos atual da GUI */
            pool_destroy(pool);
            pool = pool_create(g_max_children);
            if (!pool) { fprintf(stderr, "Erro ao recriar pool.\n"); break; }

            free(tiles);
            tiles    = generate_tiles(&params, &n_tiles);
            next_tile  = 0;
            done_tiles = 0;
            rendering  = true;

            /* Limpa buffers para a nova região */
            memset(g_pixel_vals, 0, (size_t)n_pixels);
            for (int i = 0; i < n_pixels; i++)
                g_pixel_colors[i] = (Color){0, 0, 0, 255};
            memset(g_glows, 0, sizeof(g_glows));
            g_pixels_changed = true;
        }

        /* 3. Recoloração instantânea (mudança de paleta via Space) */
        if (need_recolor)
            recolor_all(n_pixels);

        /* 4. Lançar novos filhos (suspenso enquanto restart está pendente) */
        if (!g_restart_pending) {
            while (next_tile < n_tiles && pool_active(pool) < g_max_children)
                launch_worker(pool, &params, &tiles[next_tile++]);
        }

        /* 5. Coletar resultados disponíveis e atualizar buffers */
        {
            TileResult result;
            while (pool_collect_ready(pool, &result)) {
                draw_tile_result(&result, params.width);
                free(result.pixels);
                done_tiles++;
            }
        }

        /* 6. Coletar filhos encerrados (WNOHANG) */
        pool_reap(pool);

        if (rendering && done_tiles >= n_tiles && pool_active(pool) == 0)
            rendering = false;

        /* 7. Desenhar frame */
        BeginDrawing();

        /* Atualizar textura apenas quando os buffers mudaram */
        if (g_pixels_changed) {
            UpdateTexture(texture, g_pixel_colors);
            g_pixels_changed = false;
        }
        DrawTexture(texture, 0, 0, WHITE);
        draw_glows(GetFrameTime());

        /* Overlay: box de seleção com preenchimento semi-transparente */
        if (g_selecting) {
            DrawRectangleRec(g_box, (Color){0, 100, 255, 60});
            DrawRectangleLinesEx(g_box, 2.0f, BLUE);

            /* Legenda no canto superior esquerdo */
            DrawText(TextFormat("Granularity: %d  (R=diminuir / T=aumentar)", (int)g_granularity),
                     10, 10, 18, WHITE);
            DrawText(TextFormat("Depth:       %d  (F=diminuir / G=aumentar)", (int)g_depth_f),
                     10, 32, 18, WHITE);
            DrawText(TextFormat("Max filhos:  %d  (V=diminuir / B=aumentar)", g_max_children),
                     10, 54, 18, WHITE);
            DrawText("Enter=confirmar  Backspace=cancelar  "
                     "WASD=mover  Shift+WASD=redimensionar  Roda=zoom",
                     10, params.height - 28, 15, LIGHTGRAY);
        }

        /* HUD: feedback visual ao ajustar depth/granularidade/filhos */
        if (!g_selecting && g_show_changes_timer > 0.0f) {
            if (g_gchanged)
                DrawText(TextFormat("Granularity: %d", (int)g_granularity),
                         params.width / 4, params.height / 2 - 80, 60, WHITE);
            if (g_dchanged)
                DrawText(TextFormat("Depth: %d", (int)g_depth_f),
                         params.width / 4, params.height / 2, 60, WHITE);
            if (g_mchanged)
                DrawText(TextFormat("Max filhos: %d", g_max_children),
                         params.width / 4, params.height / 2 + 80, 60, WHITE);
        }

        /* Barra de título: progresso ou instruções */
        {
            char title[256];
            if (rendering || g_restart_pending)
                snprintf(title, sizeof(title),
                         "Mandelbrot — %d/%d tiles  (filhos ativos: %d)",
                         done_tiles, n_tiles, pool_active(pool));
            else
                snprintf(title, sizeof(title),
                         "Mandelbrot — concluído | "
                         "Enter=selecionar  Space=cor  Ctrl+Z=desfazer");
            SetWindowTitle(title);
        }

        EndDrawing();

        /* Economizar CPU quando ocioso */
        if (!rendering && !g_restart_pending && !g_selecting)
            WaitTime(0.05);
    }

    /* --- Limpeza --- */
    pool_destroy(pool);
    free(tiles);
    free(g_pixel_vals);
    free(g_pixel_colors);
    UnloadTexture(texture);
    CloseWindow();
    return 0;
}
