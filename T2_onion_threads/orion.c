/*
 * orion.c — Missão Orion: Pipeline de Telemetria Espacial
 *
 * INF01142 — Sistemas Operacionais — 2026/1
 *
 * Cenário:
 *   N threads "Orion"  →  buffer_orion_lua  →  1 thread "Relay Lunar"
 *                                               (consome + processa + produz)
 *                       buffer_lua_terra    →  1 thread "Terra"
 *
 * Este arquivo contém a lógica completa do programa, MAS SEM NENHUM
 * controle de concorrência. Execute-o e observe falhas, dados corrompidos
 * ou deadlock. Sua tarefa é identificar todas as regiões críticas e
 * adicionar a sincronização correta usando pthreads (mutex + semáforos).
 *
 * Uso:
 *   ./orion <n_orions> <buf_orion_lua> <buf_lua_terra> <n_pacotes>
 *
 * Exemplo:
 *   ./orion 5 8 4 100
 *
 * REGRAS:
 *   - Não altere a lógica do programa (cálculos, prints, estruturas).
 *   - Adicione apenas declarações de variáveis de sincronização e as
 *     chamadas correspondentes (lock/unlock, wait/post).
 *   - Justifique cada decisão no relatório.
 */

#define _XOPEN_SOURCE 600
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>

/* =========================================================================
 * Estruturas de dados
 * ========================================================================= */

/* Pacote de telemetria produzido pela espaçonave Orion */
typedef struct {
  pthread_t  orion_id;        /* identificador da thread Orion origem     */
  int        seq;             /* número de sequência do pacote            */
  double     temperatura;     /* leitura do sensor de temperatura (°C)    */
  double     pressao;         /* leitura do sensor de pressão (kPa)       */
  double     posicao[3];      /* coordenadas X, Y, Z (UA)                 */
} Pacote;

/* Pacote processado pelo relay lunar — inclui metadados adicionados */
typedef struct {
  Pacote original;        /* pacote recebido da Orion                 */
  unsigned int checksum;  /* checksum calculado pelo relay            */
  long timestamp_lunar;   /* instante de recepção na Lua (ms)        */
  int  prioridade;        /* 0=normal, 1=alerta de emergência         */
} PacoteRelay;

/* Buffer circular genérico — usado para ambos os estágios */
typedef struct {
  void  *dados;           /* array de elementos (alocado em main)     */
  int    capacidade;      /* número máximo de elementos               */
  int    tamanho;         /* elemento em bytes                        */
  int    in;              /* índice de inserção                       */
  int    out;             /* índice de remoção                        */
  int    count;           /* elementos atualmente no buffer           */

  // mutex e semaforos do buffer
  pthread_mutex_t mutex;
  sem_t empty;
  sem_t full_normal; 
  sem_t full_alerta;
  sem_t full_total; // criado para resolver o problema de deadlock do sem_trywait
} Buffer;

/* Contexto global compartilhado entre todas as threads */
typedef struct {
  Buffer  buf_orion_lua;   /* estágio 1: Orions → Relay              */
  Buffer  buf_lua_terra;   /* estágio 2: Relay  → Terra              */
  int     n_pacotes;       /* pacotes que cada Orion deve enviar      */
  int     n_orions;        /* número de threads Orion                 */

  /* Contadores para verificação de corretude */
  long    total_enviados;  /* incrementado por cada Orion             */
  long    total_relay;     /* incrementado pelo Relay                 */
  long    total_recebidos; /* incrementado pela Terra                 */

  // mutex dos contadores globais
  pthread_mutex_t mutex_enviados;
  pthread_mutex_t mutex_relay;
  pthread_mutex_t mutex_recebidos;
} Contexto;

/* =========================================================================
 * Utilitários
 * ========================================================================= */

/* Retorna timestamp em milissegundos */
static long agora_ms(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Calcula checksum simples dos bytes de um Pacote */
static unsigned int calcular_checksum(const Pacote *p)
{
  unsigned int soma = 0;
  const unsigned char *b = (const unsigned char *)p;
  for (size_t i = 0; i < sizeof(Pacote); i++)
    soma += b[i];
  return soma;
}

double gerar_temperatura(void)
{
  // ignore eventuais problemas de geração de números aleatórios em multi-threaded
  double r = (double)rand() / RAND_MAX;
  if (r < 0.05)
    return 80.0 + (double)rand() / RAND_MAX * 20.0;
  else
    return 20.0 + (double)rand() / RAND_MAX * 60.0;
}

double gerar_pressao(void)
{
  // ignore eventuais problemas de geração de números aleatórios em multi-threaded  
  double r = (double)rand() / RAND_MAX;
  if (r < 0.05)
    return 80.0 + (double)rand() / RAND_MAX * 20.0;
  else
    return 0.0 + (double)rand() / RAND_MAX * 80.0;
}

/* =========================================================================
 * Operações de buffer
 * ========================================================================= */
/* buffer_init: inicializa o buffer. */
static void buffer_init(Buffer *buf, int capacidade, int tamanho_elemento)
{
  buf->dados      = malloc(capacidade * tamanho_elemento);
  buf->capacidade = capacidade;
  buf->tamanho    = tamanho_elemento;
  buf->in         = 0;
  buf->out        = 0;
  buf->count      = 0;

  // inicializa o mutex e os semaforos
  pthread_mutex_init(&buf->mutex, NULL);
  sem_init(&buf->empty, 0, capacidade);
  sem_init(&buf->full_normal, 0, 0);
  sem_init(&buf->full_alerta, 0, 0);
  sem_init(&buf->full_total, 0, 0);
}

/* buffer_destruir: finaliza o buffer. */
static void buffer_destruir(Buffer *buf)
{
  pthread_mutex_destroy(&buf->mutex);
  sem_destroy(&buf->empty);
  sem_destroy(&buf->full_normal);
  sem_destroy(&buf->full_alerta);
  sem_destroy(&buf->full_total);
  free(buf->dados);
}

/* buffer_inserir: insere elemento no buffer circular. */
static void buffer_inserir(Buffer *buf, const void *elemento)
{
  char *destino = (char *)buf->dados + buf->in * buf->tamanho;
  memcpy(destino, elemento, buf->tamanho);
  buf->in = (buf->in + 1) % buf->capacidade;
  buf->count++;
}

/* buffer_remover: remove elemento do buffer circular. */
static void buffer_remover(Buffer *buf, void *destino)
{
  char *origem = (char *)buf->dados + buf->out * buf->tamanho;
  memcpy(destino, origem, buf->tamanho);
  buf->out = (buf->out + 1) % buf->capacidade;
  buf->count--;
}

/* =========================================================================
 * Threads
 * ========================================================================= */

/*
 * orion: produtora de telemetria.
 *
 * Cada Orion envia n_pacotes pacotes ao relay lunar via buf_orion_lua.
 * Delay de rádio simulado por usleep (comunicação Orion -> Lua é lenta).
 *
 */
static void *orion(void *arg)
{
  Contexto *ctx = (Contexto *)arg;

  /* Identificação desta thread Orion */
  pthread_t meu_id = pthread_self();

  for (int seq = 0; seq < ctx->n_pacotes; seq++) {

    /* Produz pacote de telemetria com dados simulados */
    Pacote p;
    p.orion_id    = meu_id;
    p.seq         = seq;
    p.temperatura = gerar_temperatura();
    p.pressao     = gerar_pressao();
    p.posicao[0]  = 1.0 + seq * 0.3;
    p.posicao[1]  = 0.5 + seq * 0.01;
    p.posicao[2]  = 0.0;

    /* Delay de transmissão via rádio (1-3 ms) */
    usleep((1 + meu_id % 3) * 1000);

    sem_wait(&ctx->buf_orion_lua.empty);
    pthread_mutex_lock(&ctx->buf_orion_lua.mutex);

    buffer_inserir(&ctx->buf_orion_lua, &p);

    pthread_mutex_unlock(&ctx->buf_orion_lua.mutex);
    if (p.temperatura > 80 || p.pressao < 50) 
      sem_post(&ctx->buf_orion_lua.full_alerta);
    else 
      sem_post(&ctx->buf_orion_lua.full_normal);
    sem_post(&ctx->buf_orion_lua.full_total);

    long total_enviados_atual;
    pthread_mutex_lock(&ctx->mutex_enviados);

    ctx->total_enviados++;
    total_enviados_atual = ctx->total_enviados;

    pthread_mutex_unlock(&ctx->mutex_enviados);

    printf("[Orion %ld] seq=%d enviado | total=%ld\n",
	   meu_id, seq, total_enviados_atual);
  }
  return NULL;
}

/*
 * relay: consumidora da Orion e produtora para a Terra.
 *
 * O relay processa pacotes em duas filas de prioridade:
 *   - Fila de ALERTAS (prioridade alta): temperatura > 80°C ou pressão < 50 kPa
 *   - Fila NORMAL (prioridade baixa): demais pacotes
 *
 */
static void *relay(void *arg)
{
  Contexto *ctx   = (Contexto *)arg;
  int total_esperado = ctx->n_orions * ctx->n_pacotes;

  for (int i = 0; i < total_esperado; i++) {

    Pacote p;

    sem_wait(&ctx->buf_orion_lua.full_total);
    if (sem_trywait(&ctx->buf_orion_lua.full_alerta) != 0) 
      sem_wait(&ctx->buf_orion_lua.full_normal);

    pthread_mutex_lock(&ctx->buf_orion_lua.mutex);

    buffer_remover(&ctx->buf_orion_lua, &p);

    pthread_mutex_unlock(&ctx->buf_orion_lua.mutex);
    sem_post(&ctx->buf_orion_lua.empty);
  
    /* Processamento: calcula checksum e timestamp */
    PacoteRelay pr;
    pr.original        = p;
    pr.checksum        = calcular_checksum(&p);
    pr.timestamp_lunar = agora_ms();

    /* Classificação de prioridade */
    pr.prioridade = (p.temperatura > 80.0 || p.pressao < 50.0) ? 1 : 0;

    if (pr.prioridade == 1)
      printf("[RELAY] *** ALERTA *** Orion=%ld seq=%d temp=%.1f pres=%.1f\n",
	     p.orion_id, p.seq, p.temperatura, p.pressao);

    /* Delay FTL simulado (muito menor que rádio) */
    usleep(20);

    sem_wait(&ctx->buf_lua_terra.empty);
    pthread_mutex_lock(&ctx->buf_lua_terra.mutex);

    buffer_inserir(&ctx->buf_lua_terra, &pr);

    pthread_mutex_unlock(&ctx->buf_lua_terra.mutex);
    if (pr.prioridade == 1)
      sem_post(&ctx->buf_lua_terra.full_alerta);
    else
      sem_post(&ctx->buf_lua_terra.full_normal);
    sem_post(&ctx->buf_lua_terra.full_total);

    pthread_mutex_lock(&ctx->mutex_relay);

    ctx->total_relay++;

    pthread_mutex_unlock(&ctx->mutex_relay);
  }
  return NULL;
}

/*
 * terra: consumidora final.
 *
 * Recebe pacotes processados pelo relay, valida checksum e exibe.
 */
static void *terra(void *arg)
{
  Contexto *ctx          = (Contexto *)arg;
  int       total_esperado = ctx->n_orions * ctx->n_pacotes;
  int       alertas        = 0;
  int       erros_checksum = 0;

  for (int i = 0; i < total_esperado; i++) {

    PacoteRelay pr;

    sem_wait(&ctx->buf_lua_terra.full_total);
    if (sem_trywait(&ctx->buf_lua_terra.full_alerta) != 0)
      sem_wait(&ctx->buf_lua_terra.full_normal);

    pthread_mutex_lock(&ctx->buf_lua_terra.mutex);

    buffer_remover(&ctx->buf_lua_terra, &pr);

    pthread_mutex_unlock(&ctx->buf_lua_terra.mutex);
    sem_post(&ctx->buf_lua_terra.empty);

    /* Validação de integridade */
    unsigned int cs = calcular_checksum(&pr.original);
    if (cs != pr.checksum) {
      erros_checksum++;
      printf("[Terra] ERRO checksum Orion=%ld seq=%d "
	     "esperado=%u recebido=%u\n",
	     pr.original.orion_id, pr.original.seq,
	     cs, pr.checksum);
    }

    if (pr.prioridade == 1) alertas++;

    pthread_mutex_lock(&ctx->mutex_recebidos);

    ctx->total_recebidos++;

    pthread_mutex_unlock(&ctx->mutex_recebidos);
  }

  printf("\n[Terra] Recepção concluída.\n");
  printf("  Pacotes recebidos : %ld\n",  ctx->total_recebidos);
  printf("  Alertas recebidos : %d\n",   alertas);
  printf("  Erros de checksum : %d\n",   erros_checksum);
  printf("  Enviados vs recebidos: %ld vs %ld\n",
	 ctx->total_enviados, ctx->total_recebidos);

  return NULL;
}

/* =========================================================================
 * main
 * ========================================================================= */
int main(int argc, char *argv[])
{
  if (argc != 5) {
    fprintf(stderr,
            "Uso: %s <n_orions> <buf_orion_lua> <buf_lua_terra> <n_pacotes>\n"
            "Exemplo: %s 5 8 4 100\n", argv[0], argv[0]);
    return 1;
  }

  int n_orions       = atoi(argv[1]);
  int buf_orion_lua  = atoi(argv[2]);
  int buf_lua_terra  = atoi(argv[3]);
  int n_pacotes      = atoi(argv[4]);

  if (n_orions < 1 || buf_orion_lua < 1 ||
      buf_lua_terra < 1 || n_pacotes < 1) {
    fprintf(stderr, "Todos os parâmetros devem ser positivos.\n");
    return 1;
  }

  /* Inicialização do contexto */
  Contexto ctx = {0};
  memset(&ctx, 0, sizeof(ctx));
  ctx.n_orions  = n_orions;
  ctx.n_pacotes = n_pacotes;

  pthread_mutex_init(&ctx.mutex_enviados, NULL);
  pthread_mutex_init(&ctx.mutex_relay, NULL);
  pthread_mutex_init(&ctx.mutex_recebidos, NULL);

  buffer_init(&ctx.buf_orion_lua, buf_orion_lua, sizeof(Pacote));
  buffer_init(&ctx.buf_lua_terra, buf_lua_terra, sizeof(PacoteRelay));

  /* Criação das threads */
  pthread_t *threads_orion = calloc(n_orions, sizeof(pthread_t));
  pthread_t  thread_relay = {0}, thread_terra = {0};

  pthread_create(&thread_terra, NULL, terra, &ctx);
  pthread_create(&thread_relay, NULL, relay, &ctx);
  for (int i = 0; i < n_orions; i++) {
    pthread_create(&threads_orion[i], NULL, orion, &ctx);
  }

  /* Aguarda término */
  for (int i = 0; i < n_orions; i++) {
    pthread_join(threads_orion[i], NULL);
  }
  pthread_join(thread_relay, NULL);
  pthread_join(thread_terra, NULL);

  /* Limpeza final */
  buffer_destruir(&ctx.buf_orion_lua);
  buffer_destruir(&ctx.buf_lua_terra);
  pthread_mutex_destroy(&ctx.mutex_enviados);
  pthread_mutex_destroy(&ctx.mutex_relay);
  pthread_mutex_destroy(&ctx.mutex_recebidos);
  free(threads_orion);
  return 0;
}
